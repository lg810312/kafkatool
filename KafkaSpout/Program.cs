using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;

namespace KafkaSpout
{
    class Program
    {
        static IConfigurationRoot config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json", false, true).Build();

        const string logCategory = "KafkaTool.KafkaSpout";
        static ILogger logger = LoggerFactory.Create(builder => builder.AddDebug().AddConsole().AddConfiguration(config)).CreateLogger(logCategory);

        const int MaxConcurrentTasks_Default = 8; //Default MaxConcurrentTasks, can be overwritten in appsettings.json

        /// <summary>
        /// Generate key of message with KeyParser. Currently only support json parser using NewtonSoft
        /// </summary>
        /// <param name="message"></param>
        /// <param name="KeyParser"></param>
        /// <param name="KeyPattern"></param>
        /// <param name="KeyPatternSeparator"></param>
        static string GenerateKey(string message, string KeyParser, string KeyPattern, string KeyPatternSeparator = "_")
        {
            if (string.IsNullOrWhiteSpace(KeyPattern)) return null;

            var KeyProperties = KeyPattern.Split(KeyPatternSeparator);
            var Properties = new List<string>();

            foreach (var KeyProperty in KeyProperties)
            {
                JToken v = JObject.Parse(message).SelectToken(KeyProperty);
                Properties.Add(v.ToString());
            }

            return Properties.Count == 0 ? null : string.Join(KeyPatternSeparator, Properties);
        }

        static void Main(string[] args)
        {
            // Kafka Config
            var KafkaSection = config.GetSection("Kafka");
            string brokerlist = KafkaSection.GetValue<string>("brokerlist");

            // Kafka producer
            var producerConfig = new ProducerConfig { BootstrapServers = brokerlist };

            // Kafka topic
            string topic = KafkaSection.GetValue<string>("topic");

            // Producing config
            var SpoutSection = config.GetSection("Spout");
            string SpoutFilePath = SpoutSection.GetValue<string>("FilePath");
            List<string> SpoutFiles = new List<string>();
            if (File.Exists(SpoutFilePath))
                SpoutFiles.Add(SpoutFilePath);
            else
            {
                string FileFolder = SpoutSection.GetValue<string>("FileFolder");

                logger.LogError($"{SpoutFilePath} not found! Try to find files in {FileFolder}");

                try
                {
                    SpoutFiles.AddRange(Directory.EnumerateFiles(FileFolder));
                }
                catch (Exception ex)
                {
                    logger.LogError(ex.Message);
                }
            }

            string SpoutFilter = SpoutSection.GetValue<string>("Filter");
            string SpoutKeyParser = SpoutSection.GetValue<string>("KeyParser").ToLower();
            if (!new string[] { "json" }.Contains(SpoutKeyParser)) { logger.LogError("KeyParser must be json!"); Environment.Exit(-1); }
            string SpoutKeyPattern = SpoutSection.GetValue<string>("KeyPattern");
            string SpoutKeyPatternSeparator = SpoutSection.GetValue<string>("KeyPatternSeparator");
            int MaxConcurrentTasks = SpoutSection.GetValue("MaxConcurrentTasks", MaxConcurrentTasks_Default);

            string MessageKey = null;
            long CountMessageSent = 0;

            var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            Parallel.ForEach(SpoutFiles, new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentTasks }, SpoutFile =>
            {
            foreach (var message in File.ReadLines(SpoutFile))
            {
                if (!string.IsNullOrWhiteSpace(SpoutFilter) && !message.Contains(SpoutFilter)) continue;

                MessageKey = GenerateKey(message, SpoutKeyParser, SpoutKeyPattern, SpoutKeyPatternSeparator);

                    Action<DeliveryReport<string, string>> handler = r =>
                    {
                        if (!r.Error.IsError)
                            logger.LogInformation($"Delivered message to {r.TopicPartitionOffset}");
                        else
                            logger.LogError($"Delivery Error: {r.Error.Reason}");
                    };

                    try
                    {
                        producer.Produce(topic, new Message<string, string> { Key = MessageKey, Value = message }, handler);
                        CountMessageSent++;
                        logger.LogInformation($"{CountMessageSent} message(s) sent");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        logger.LogError($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                }
            });

            // Producer asychronously send data, so await data to be sent completely
            producer.Flush(TimeSpan.FromSeconds(1000));
        }
    }
}
