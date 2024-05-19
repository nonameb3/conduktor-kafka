# start run command
kafka-console-producer

# producing
kafka-console-producer --bootstrap-server 0.0.0.0:9092 --topic frist-topic

# producing with property
kafka-console-producer --bootstrap-server 0.0.0.0:9092 --topic frist-topic --producer-property acks=all

# producing with key,  [key:value] ex,mykey:messasge
kafka-console-producer --bootstrap-server 0.0.0.0:9092 --topic frist-topic --property parse.key=true --property key.separator=:
