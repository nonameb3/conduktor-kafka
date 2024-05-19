# start run command
kafka-topic

# create topic with SSL
kafka-topics --create --topic frist-topic --bootstrap-server 0.0.0.0:9092 --command-config ./playground.config

# create topic with no SSL
kafka-topics --create --topic frist-topic --bootstrap-server 0.0.0.0:9092

# create topic with partitions and replication factor
kafka-topics --create --topic partitions-topic --bootstrap-server 0.0.0.0:9092 --partitions 5 --replication-factor 1

# list topic
kafka-topics --list partitions-topic --bootstrap-server 0.0.0.0:9092

# get detail topic
kafka-topics --describe --topic frist-topic partitions-topic --bootstrap-server 0.0.0.0:9092

# delete topic
kafka-topics --delete --topic frist-topic partitions-topic --bootstrap-server 0.0.0.0:9092
