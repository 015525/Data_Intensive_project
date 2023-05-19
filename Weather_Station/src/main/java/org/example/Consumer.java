package org.example;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.example.avro.WeatherMessage;

public class Consumer {
    private static final String TOPIC_NAME = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "weather-group";

    public static void main(String[] args) {
        // Configure Kafka consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
       // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Create a Kafka consumer
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // Start consuming messages
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    byte[] valueBytes = record.value();
                    WeatherMessage value = deserializeAvroMessage(valueBytes);
                    String key = record.key();
                    System.out.println("Received Avro message: key: " + key + " \n value: " + value);
                }catch(Exception e){
                    System.err.println("failed to deserialize Avro message");
                    e.printStackTrace();
                }
            }
        }
    }


    public static WeatherMessage deserializeAvroMessage(byte[] bytes) throws IOException {

        SpecificDatumReader<WeatherMessage> reader = new SpecificDatumReader<>(WeatherMessage.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        WeatherMessage message = reader.read(null, decoder);

        return message;
    }

}
