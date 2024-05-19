package io.conduktor.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.beans.EventHandler;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("hello world");

        String bootstrapServer = "localhost:19092";
        String topic = "wikimedia.change";

        // config kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // config safe producer, kafka version < 3.x
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // fix send dup data when network error
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // retry

        // config high throughput producer (performance CPU)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 *1024));

        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // crete steam builder
        EventSource.Builder builder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder backgroundBuilder = new BackgroundEventSource.Builder(eventHandler, builder);
        BackgroundEventSource eventSource = backgroundBuilder.build();
        //  EventSource eventSource = builder.build();

        // start producer
        eventSource.start();

        // sleep
        TimeUnit.MINUTES.sleep(10);
    }
}
