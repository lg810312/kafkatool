# KakfaExtractor

KakfaExtractor is a tool that can extract messages from Kafka. It retrieves messages and saves them to a data file.

## Setting

appsettings.json is the setting file. Important setting items are as below.

Extraction: settings about extraction
Extraction.Path: path to which the data file is saved
Extraction.FileName: filename of the data file, {} will be replaced with datetime of extraction
Extraction.Filter: Only messages containing filter are saved while others are ignored. Not effective when partition list has 1 and offset list is not empty. If not specified or not effective, no messages will be ignored.

Kafka: settings about Kafka
Kafka.brokerlist: broker servers, also called Bootstrap Servers
Kafka.topic: topic from which messages are retrieved
Kafka.partitionlist: partition list for the tool to retrieve messages, if empty, all partitions will be in the list
Kafka.offsetlist: Offset is one in a partition not a topic. Offset list is one for the tool to retrieve messages in partition list from specified offsets. If empty or partition list has more than 1, the setting will not be effective and messages will be extracted from beginning in a partition.

setting snippet sample:

```json
  "Extraction": {
    "Path": ".\\extraction",
    "FileName": "Extraction{0:yyyyMMddHHmmss}.txt",
    "Filter": "50chn"
  },
  "Kafka": {
    "brokerlist": "10.253.204.36:9092,10.253.204.37:9092,10.253.204.38:9092",
    "topic": "ConsumerBestRecordTopic",
    "partitionlist": "0,1,2,3,4,5,6,7,8,9",
    "offsetlist": "1001,2002,3003,4004"
  }
```
