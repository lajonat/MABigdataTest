package com.test.Consumer;

import com.test.Processor.ProcessException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * General purpose Kafka consumer - reads records (assumes key is timestamp, value is JSON string), passes them to the attached processor
 * Improvements to be made:
 *      - Multithreading support
 *      - Better handling of errors and exceptions
 *      - Offset management (if needed, batching and such)
 *      - Read from multiple topics in same process?
 *      - Use multiple processors on same record?
 */
public class KafkaTopicConsumer extends BaseConsumer {

    public void Start() {
        KafkaConsumer<Long, String> consumer = connectToKafka();
        System.out.println(String.format("Started reading from kafka (topic %s, group %s)", topicName, group));

        while (!Thread.interrupted()) {

            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Long, String> record : records) {
                try {
                    processor.Process(record.value());
                } catch (ProcessException e) {
                    // LOG HERE
                    // ALERT IF NEEDED
                    System.err.println("Processor error: " + e.getMessage());
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private KafkaConsumer<Long, String> connectToKafka() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }
}
