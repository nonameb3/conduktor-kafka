# start run command
kafka-consumer-groups

# list group
kafka-consumer-groups --list frist-topic --bootstrap-server 0.0.0.0:9092

# get detail group
kafka-consumer-groups --describe --group app --bootstrap-server 0.0.0.0:9092

# delete group
kafka-consumer-groups --delete --topic frist-topic partitions-topic --bootstrap-server 0.0.0.0:9092
