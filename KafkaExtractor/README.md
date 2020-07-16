# KakfaExtractor

KakfaExtractor is a tool that can extract messages from Kafka. It retrieves messages and saves them to a data file.

## Setting

appsettings.json is the setting file. Important setting items are as below.

- Extraction: settings about extraction
- Extraction.Path: path to which the data file is saved
- Extraction.FileName: filename of the data file, {} will be replaced with datetime of extraction
- Extraction.Filter: only messages containing filter are saved while others are ignored. Not effective when partition list has 1 and offset list is not empty. If not specified or not effective, no messages will be ignored.
- Extraction.MessageWriteBufferSize: capacity of write buffer. The unit is count of messages. Once the buffer is full, messages in the buffer are written to file and then the buffer is cleared.
- Extraction.MaxMessageFileSize: max file size of message file. The unit is byte. Once a message file size reaches or exceeds the number, a new message file is used to store remaining unsaved messages.
- Extraction.TimeAfter: Messages with timestamp after the specified time are extracted. Otherwise they are ignored. If empty or invalid, the time is 0001-01-01.
- Extraction.MaxConcurrentTasks: the number of tasks concurrently running to consume messages from Kafka. It is effective when it is 2 or more and not greater than partition count.

- Kafka: settings about Kafka
- Kafka.brokerlist: broker servers, also called Bootstrap Servers
- Kafka.topic: topic from which messages are retrieved
- Kafka.partitionlist: partition list for the tool to retrieve messages, if empty, all partitions will be in the list
- Kafka.offsetlist: Offset is one in a partition not a topic. Offset list is one for the tool to retrieve messages in partition list from specified offsets. If empty or partition list has more than 1, the setting will not be effective and messages will be extracted from beginning in a partition.

setting snippet sample:

```json
  "Extraction": {
    "Path": ".\\extraction",
    "FileName": "Extraction{0:yyyyMMddHHmmss}.txt",
    "Filter": "50chn",
    "MessageWriteBufferSize": 5000,
    "MaxMessageFileSize": 1073741824,
    "TimeAfter": "2020-01-01",
    "MaxConcurrentTasks": 8
  },
  "Kafka": {
    "brokerlist": "10.253.204.36:9092,10.253.204.37:9092,10.253.204.38:9092",
    "topic": "ConsumerBestRecordTopic",
    "partitionlist": "0,1,2,3,4,5,6,7,8,9",
    "offsetlist": "1001,2002,3003,4004"
  }
```
## Change Log

- 2019-11-20 First stable version
- 2020-04-16 Asynchronously write messages to file. Once the message file size reaches or exceeds the specified number, messages are written to another file.
- 2020-06-18 Able to consume messages with timestamp after the specified time.
- 2020-07-15 Update denpendencies to their latest version. Add setting items to set write buffer size, max message file size and max concurrent task count.