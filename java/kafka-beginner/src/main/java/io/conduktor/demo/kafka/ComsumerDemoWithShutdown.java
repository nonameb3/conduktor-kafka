package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ComsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ComsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer demo starting");

        String group_id = "demo_java_consumer";

        String topic = "demo_java";

        // create property
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", group_id);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        final  Thread mainThread = Thread.currentThread();

        // config shutdown hooks
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                super.run();

                log.warn("Detected a shut down, exit by calling consumer.wakeup()");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // consumer processing
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key " + record.key() + " Value " + record.value());
                    log.info("Partition " + record.partition() + " Offset " + record.offset());
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer starting to shut down");
            consumer.close(); // commit offset and close
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer");
        } finally {
            log.warn("Consumer is now gracefully shut down");
        }
    }
}
