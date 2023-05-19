package org.example;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;


import java.io.IOException;
import java.util.Timer;
import java.util.Random;
import java.util.TimerTask;
import java.util.Properties;
import java.io.ByteArrayOutputStream;
import org.example.avro.WeatherMessage;
import org.example.avro.weather_fields;


public class Station {
    private static final String TOPIC_NAME = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Producer
        long id = new Random().nextLong(); //id of the station

        //producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new KafkaMessageTask(producer, id), 0, 1000);

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            timer.cancel();
            producer.close();
        }));
    }


    private static class KafkaMessageTask extends TimerTask{
        private final KafkaProducer<String, byte[]> producer;
        private final long id;
        private final Random random = new Random();

        private final SpecificDatumWriter<WeatherMessage> writer = new SpecificDatumWriter<>(WeatherMessage.getClassSchema());
        private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        private KafkaMessageTask(KafkaProducer<String, byte[]>producer, long id) {
            this.producer= producer;
            this.id = id;
        }


        static long sNo = 1;
        @Override
        public void run() {
            WeatherMessage message = WeatherMessage.newBuilder()
                    .setStationId(this.id)
                    .setSNo(sNo)
                    .setBatteryStatus(getStatus())
                    .setStatusTimestamp(System.currentTimeMillis())
                    .setWeather(weather_fields.newBuilder()
                            .setHumidity(random.nextInt(100))
                            .setTemperature(random.nextInt(135))
                            .setWindSpeed(random.nextInt(410))
                            .build())
                    .build();

            try {
                writer.write(message, encoder);
                encoder.flush();
                byte[] serializedBytes = outputStream.toByteArray();
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC_NAME, Long.toString((sNo++)), serializedBytes);


                /**
                 * only in case we need only callback response from the prodcuer we can un-comment that part
                 */
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Error sending message: " + exception.getMessage());
                        } else {
                            System.out.println("Message sent successfully. Offset: " + metadata.offset() + " with service Number = "+ (sNo-1));
                        }
                    }
                });
            }
            catch (IOException e){
                e.printStackTrace();
            }
            finally {
                try {
                    outputStream.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
                outputStream.reset();
            }
        }
    }

    public static String getStatus(){
        String status;

        Random random = new Random();
        int percentage = random.nextInt(100);

        if(percentage < 30)
            return "low";

        else if (percentage < 70)
            return "medium";

        else
            return "high";
    }
}



