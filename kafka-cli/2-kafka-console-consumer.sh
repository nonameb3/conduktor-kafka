# start run command
kafka-console-consumer

# consuming on current time
kafka-console-consumer --bootstrap-server 0.0.0.0:9092 --topic frist-topic

# consuming from beginning
kafka-console-consumer --bootstrap-server 0.0.0.0:9092 --topic frist-topic --from-beginning

# producing with key,  [key:value] ex,mykey:messasge
kafka-console-consumer --bootstrap-server 0.0.0.0:9092 --topic frist-topic --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property print.partition=true --property print.timestamp=true

########################
# consumer group
########################

# consuming with group
kafka-console-consumer --bootstrap-server 0.0.0.0:9092 --topic frist-topic --group app

