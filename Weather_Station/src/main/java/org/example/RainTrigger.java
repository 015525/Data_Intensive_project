package org.example;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.avro.WeatherMessage;

import java.io.IOException;
import java.util.Properties;

public class RainTrigger {

    public static void main(String[] args){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "RAIN_TRIGGER");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //config.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-group");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //KStream<String, byte[]> message = builder.stream( "weather-topic");

        builder.stream("weather-topic", Consumed.with(Serdes.String(), Serdes.ByteArray()))
                .mapValues(value-> {
                    try {
                        return deserializeAvroMessage(value);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter((key, decodedValue) -> decodedValue.getWeather().getHumidity() > 70)
                .mapValues(decodedValue -> " this is station with id " + decodedValue.getStationId() + ", " + decodedValue.getSNo()+ " it's going to rain here")
                .to("rain_warning", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }


    public static WeatherMessage deserializeAvroMessage(byte[] bytes) throws IOException {

        SpecificDatumReader<WeatherMessage> reader = new SpecificDatumReader<>(WeatherMessage.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        WeatherMessage message = reader.read(null, decoder);

        return message;
    }

}
