﻿using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace KafkaExtractor
{
    class Program
    {
        static readonly IConfigurationRoot config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json", false, true).Build();

        const string logCategory = "KafkaTool.KafkaExtractor";
        static readonly ILogger logger = LoggerFactory.Create(builder => builder.AddDebug().AddConsole().AddConfiguration(config)).CreateLogger(logCategory);

        static System.Collections.Concurrent.ConcurrentQueue<string> MessageList = new System.Collections.Concurrent.ConcurrentQueue<string>();
        static long MessageExtractedCount = 0;
        static bool ExtractionInProgress;
        const uint MessageWriteBufferSize_Default = 5000; //Default WriteBuffer, can be overwritten in appsettings.json
        static uint MessageWriteBufferSize;
        const uint MaxMessageFileSize_Default = uint.MaxValue; //Default MaxMessageFileSize, can be overwritten in appsettings.json
        static uint MaxMessageFileSize;
        const int MaxConcurrentTasks_Default = 8; //Default MaxConcurrentTasks, can be overwritten in appsettings.json

        /// <summary>
        /// consumer group functionality (i.e. Subscribe + offset commits) is not used.
        /// the consumer is manually assigned to a partition and always starts consumption
        /// from a specific offset.
        /// </summary>
        static string AssignManually(ConsumerConfig consumerConfig, string topic, int partition, long offset)
        {
            string message = null;

            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).SetErrorHandler((_, e) => logger.LogError($"Error: {e.Reason}")).Build())
            {
                consumer.Assign(new TopicPartitionOffset(topic, partition, offset));

                try
                {
                    var consumeResult = consumer.Consume();
                    // Note: End of partition notification has not been enabled, so
                    // it is guaranteed that the ConsumeResult instance corresponds
                    // to a Message, and not a PartitionEOF event.
                    message = consumeResult.Message.Value;
                    logger.LogInformation($"Received message at {consumeResult.TopicPartitionOffset}");
                }
                catch (ConsumeException e)
                {
                    logger.LogError($"Consume error: {e.Error.Reason}");
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }

                MessageExtractedCount++;
                return message;
            }
        }

        /// <summary>
        /// Extract messages using filter after specified time
        /// </summary>
        /// <param name="consumerConfig"></param>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        /// <param name="filter"></param>
        /// <param name="timeAfter"></param>
        static void AssignManually(ConsumerConfig consumerConfig, string topic, int partition, long offset, string filter, DateTime timeAfter)
        {
            string message = null;
            long ExtractedCountWithFilter = 0;

            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).SetErrorHandler((_, e) => logger.LogError($"Error: {e.Reason}")).Build())
            {
                // Get last offset of specified partition
                long lastoffset = consumer.QueryWatermarkOffsets(new TopicPartition(topic, new Partition(partition)), TimeSpan.FromSeconds(10)).High - 1;

                // Extract messages from offset
                consumer.Assign(new TopicPartitionOffset(topic, partition, offset));
                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));
                            // Note: End of partition notification has not been enabled, so
                            // it is guaranteed that the ConsumeResult instance corresponds
                            // to a Message, and not a PartitionEOF event.
                            if (consumeResult.Message.Timestamp.UtcDateTime.CompareTo(timeAfter) < 0)
                            {
                                logger.LogInformation($"Skip message at {consumeResult.TopicPartitionOffset} as it is ealier than {timeAfter}");
                                continue;
                            }
                            message = consumeResult.Message.Value;
                            logger.LogInformation($"Received message at {consumeResult.TopicPartitionOffset}");
                            if (string.IsNullOrWhiteSpace(filter) || message.Contains(filter)) // if filter is actually empty or message contains filter, extract the message
                            {
                                MessageList.Enqueue(message);
                                ExtractedCountWithFilter++;
                                MessageExtractedCount++;
                                logger.LogInformation("Message extracted");
                            }
                            if (consumeResult.TopicPartitionOffset.Offset == lastoffset) break;
                        }
                        catch (ConsumeException e)
                        {
                            logger.LogError($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    logger.LogInformation("Closing consumer.");
                    consumer.Close();
                }
            }

            logger.LogInformation($"{ExtractedCountWithFilter} extracted from partition {partition}");
        }

        /// <summary>
        /// Async write messages to file
        /// </summary>
        /// <param name="FilePath"></param>
        /// <param name="FileName"></param>
        static void WriteMessageToFileAsync(string FilePath, string FileName)
        {
            string message = null;
            var messagelist = new List<string>();

            //Wait for messages added to queue
            Thread.Sleep(5000);

            var FullPath = Path.Combine(FilePath, string.Format(FileName, DateTime.Now));

            while (ExtractionInProgress || !MessageList.IsEmpty)
            {
                messagelist.Clear();
                while (!MessageList.IsEmpty && messagelist.Count < MessageWriteBufferSize)
                    if (MessageList.TryDequeue(out message)) messagelist.Add(message);

                // Save to extraction file
                if (messagelist.Count > 0)
                    try
                    {
                        File.AppendAllLines(FullPath, messagelist);

                        if (new FileInfo(FullPath).Length > MaxMessageFileSize)
                        {
                            FileName = config.GetSection("Extraction").GetValue<string>("FileName");
                            FullPath = Path.Combine(FilePath, string.Format(FileName, DateTime.Now));
                        }

                        logger.LogInformation($"Successfully save extraction file {FullPath}!");
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, $"Fail to save extraction file {FullPath}!");
                    }

                Thread.Sleep(500);
            }
        }

        /// <summary>
        /// Read extraction list file
        /// Line format: topic,partition,offset[,IsAll]
        /// IsAll, 0: only retrieve the data at offset, 1+: retrieve all from offset
        /// </summary>
        /// <param name="FullPath"></param>
        /// <returns></returns>
        static List<Tuple<string,int,long, bool>> ReadExtractionList(string FullPath)
        {
            var extractionList = new List<Tuple<string, int, long, bool>>();
            if (string.IsNullOrWhiteSpace(FullPath)) return extractionList;

            try
            {
                foreach (var line in File.ReadAllLines(FullPath))
                {
                    var extrationitem = line.Split(',');
                    extractionList.Add(new Tuple<string, int, long, bool>(extrationitem[0], int.Parse(extrationitem[1]), long.Parse(extrationitem[2]), extrationitem.Length > 3 && byte.Parse(extrationitem[3]) > 0));
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);
            }

            return extractionList;
        }

        static void Main(string[] args)
        {
            // Kafka Config
            var KafkaSection = config.GetSection("Kafka");
            string brokerlist = KafkaSection.GetValue<string>("brokerlist");

            // Kafka consumer
            ConsumerConfig consumerConfig = new ConsumerConfig
            {
                // the group.id property must be specified when creating a consumer, even 
                // if you do not intend to use any consumer group functionality.
                GroupId = new Guid().ToString(),
                BootstrapServers = brokerlist,
                // partition offsets can be committed to a group even by consumers not
                // subscribed to the group. in this example, auto commit is disabled
                // to prevent this from occurring.
                EnableAutoCommit = false
            };

            // Kafka topic
            var topic = KafkaSection.GetValue<string>("topic");

            // Kafka topic partition list
            var partitions = KafkaSection.GetValue<string>("partitionlist");
            var partitionlist = string.IsNullOrWhiteSpace(partitions) ? new List<int>() : partitions.Split(',').Select(p => int.Parse(p)).ToList();
            if (partitionlist.Count == 0) // if partition is not specified, extract all partitions
                using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerlist }).Build())
                {
                    // Warning: The API for this functionality is subject to change.
                    var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                    var topicmeta = meta.Topics.First(t => t.Topic == topic);
                    if (topicmeta.Equals(null))
                        logger.LogError($"Topic: {topic} {topicmeta.Error}");
                    else
                        partitionlist = topicmeta.Partitions.Select(p => p.PartitionId).ToList();
                }
            logger.LogInformation($"{partitionlist.Count} partitions are to be extracted");

            // Kafka topic partition offset list
            var offsets = KafkaSection.GetValue<string>("offsetlist");
            var offsetlist = string.IsNullOrWhiteSpace(offsets) ? new List<long>() : offsets.Split(',').Select(o => long.Parse(o)).ToList();
            // if messages from multiple partitions are to be extracted or offset is not specified, offset will be from beginning 
            if (partitionlist.Count != 1 || offsetlist.Count == 0) offsetlist = new long[1] { Offset.Beginning }.ToList();

            // Kafka extraction list
            var extractionListFile = KafkaSection.GetValue<string>("extractionlist");
            var extractionList = ReadExtractionList(extractionListFile);

            // Extraction config
            var ExtractionSection = config.GetSection("Extraction");
            var ExtractionPath = ExtractionSection.GetValue<string>("Path");
            if (!Directory.Exists(ExtractionPath)) Directory.CreateDirectory(ExtractionPath);
            var ExtractionFileName = ExtractionSection.GetValue<string>("FileName");
            ExtractionFileName = string.Format(ExtractionFileName, DateTime.Now);
            var ExtractionFilter = ExtractionSection.GetValue<string>("Filter");
            // If offsetlist has more than 1, filter is ignored.
            if (offsetlist.Count > 1) ExtractionFilter = string.Empty;
            MessageWriteBufferSize = ExtractionSection.GetValue("MessageWriteBufferSize", MessageWriteBufferSize_Default);
            if (MessageWriteBufferSize == 0) MessageWriteBufferSize = MessageWriteBufferSize_Default;
            MaxMessageFileSize = ExtractionSection.GetValue("MaxMessageFileSize", MaxMessageFileSize_Default);
            if (MaxMessageFileSize == 0) MaxMessageFileSize = MaxMessageFileSize_Default;
            var timeAfter = ExtractionSection.GetValue("TimeAfter", DateTime.MinValue);
            int MaxConcurrentTasks = ExtractionSection.GetValue("MaxConcurrentTasks", MaxConcurrentTasks_Default);

            // Start to extract
            MessageExtractedCount = 0;
            ExtractionInProgress = true;

            // Start a thread to async writing file
            var WriteMessage = new Thread(() => WriteMessageToFileAsync(ExtractionPath, ExtractionFileName));
            WriteMessage.Start();

            // Start to extract messages from Kafka
            if (extractionList.Count > 0) // if extractcionList has items
                Parallel.ForEach(extractionList, new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentTasks }, extractionitem =>
                {
                    if (extractionitem.Item4)
                        AssignManually(consumerConfig, extractionitem.Item1, extractionitem.Item2, extractionitem.Item3, ExtractionFilter, DateTime.MinValue);
                    else
                        MessageList.Enqueue(AssignManually(consumerConfig, extractionitem.Item1, extractionitem.Item2, extractionitem.Item3));
                });
            else
                Parallel.ForEach(partitionlist, new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentTasks }, partition =>
                    {
                        if (offsetlist.Count > 1)
                            offsetlist.ForEach(offset => MessageList.Enqueue(AssignManually(consumerConfig, topic, partition, offset)));
                        else
                            AssignManually(consumerConfig, topic, partition, offsetlist[0], ExtractionFilter, timeAfter);
                    });

            // Extraction completed
            ExtractionInProgress = false;
            while (WriteMessage.IsAlive) Thread.Sleep(10);

            logger.LogInformation($"{MessageExtractedCount} messages are extracted");
            logger.LogInformation("Extraction and saving completed!");
        }
    }
}
