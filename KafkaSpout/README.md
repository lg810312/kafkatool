# KakfaSpout

KakfaSpout is a tool that can send messages to Kafka. It reads a data file contains a message in each line and sends message to Kafka.

## Setting

appsettings.json is the setting file. Important setting items are as below.

- Spout: settings about producing messages to Kafka
- Spout.FilePath: the data file contains a message in each line
- Spout.FileFolder: the folder where the data files exist
- Spout.Filter: message that contains filter can be sent while others are skipped, if not specified, no messages will be skipped
- Spout.KeyParser: Parser by which message can be parsed for message key generation, currently only json parser is supported
- Spout.KeyPattern: Pattern with which message key is generated from message. As to json, json properties are extracted using json path with separator
- Spout.KeyPatternSeparator: Separator with which message values extracted from message using pattern are splitted for the tool to identify values
For example, message is { MarketCode: "CHN", BrandCode: "01" }, pattern is $.MarketCode_$.BrandCode and separator is _, then message key is CHN_01.
- Spout.MaxConcurrentTasks: the number of tasks concurrently running to produce messages to Kafka. It is effective when concurrently processing 2 or more data files.

- Kafka: settings about Kafka
- Kafka.brokerlist: broker servers, also called Bootstrap Servers
- Kafka.topic: topic to which messages are sent

## ChangeLog

2019-11-20 First stable version
2020-07-20 Support read files from a specified folder