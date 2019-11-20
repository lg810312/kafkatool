using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace KafkaSpout
{
    class Program
    {
        static IConfigurationRoot config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json", false, true).Build();

        const string logCategory = "KafkaTool.KafkaSpout";
        static ILogger logger = LoggerFactory.Create(builder => builder.AddDebug().AddConsole().AddConfiguration(config)).CreateLogger(logCategory);

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
            var topic = KafkaSection.GetValue<string>("topic");

            // Producing config
            var SpoutSection = config.GetSection("Spout");
            var SpoutFilePath = SpoutSection.GetValue<string>("FilePath");
            if (!File.Exists(SpoutFilePath)) { logger.LogError($"{SpoutFilePath} not found"); Environment.Exit(-1); }
            var SpoutFilter = SpoutSection.GetValue<string>("Filter");
            var SpoutKeyParser = SpoutSection.GetValue<string>("KeyParser").ToLower();
            if (!new string[] { "json" }.Contains(SpoutKeyParser)) { logger.LogError("KeyParser must be json!"); Environment.Exit(-1); }
            var SpoutKeyPattern = SpoutSection.GetValue<string>("KeyPattern");
            var SpoutKeyPatternSeparator = SpoutSection.GetValue<string>("KeyPatternSeparator");

            string MessageKey = null;
            long CountMessageSent = 0;
            using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
            {
                foreach (var message in File.ReadLines(SpoutFilePath))
                {
                    if (!string.IsNullOrWhiteSpace(SpoutFilter) && !message.Contains(SpoutFilter)) continue;

                    MessageKey = GenerateKey(message, SpoutKeyParser, SpoutKeyPattern, SpoutKeyPatternSeparator);

                    producer.ProduceAsync(topic, new Message<string, string> { Key = MessageKey, Value = message });

                    CountMessageSent++;
                    logger.LogInformation($"{CountMessageSent} message(s) sent");
                }

                // As the Tasks returned by ProduceAsync are not waited on there will still be messages in flight.
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
