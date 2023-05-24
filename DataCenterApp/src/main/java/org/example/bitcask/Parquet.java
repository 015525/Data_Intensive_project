package org.example.bitcask;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

public class Parquet {

    private String path;
    //private ArrayList<Record> recordsBuffer = new ArrayList<>();
    private HashMap<Long, ArrayList<GenericRecord>> recordsBuffer = new HashMap<>();
    private HashMap<Long, Long> recordsNum = new HashMap<>();

    private Object writeParqLock = new Object();

    Parquet(){
        String project_path = System.getProperty("user.dir");
        this.path = project_path + File.separator +  "parquet_files";

        //System.out.println("folder_path : " + path);
        File folder = new File(path);
        if (!folder.exists()){
            boolean created = folder.mkdir();
        }
    }

    public void makeParqDir(long station_id){
        String stationPath = this.path + File.separator + station_id;
        File folderStation = new File(stationPath);
        if (!folderStation.exists()){
            boolean created = folderStation.mkdir();
        }
    }

    public void handle_rec(Record rec){
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (writeParqLock) {
                    makeParqDir(rec.station_id);
                    ArrayList<GenericRecord> recs = recordsBuffer.get(rec.station_id);
                    if (recs == null) {
                        recs = new ArrayList<>();
                    }
                    recs.add(createRecord(rec));
                    recordsBuffer.put(rec.station_id, recs);
                    if (recs.size() >= 10000){
                        write_parquet(rec.station_id, recs);
                        recordsBuffer.remove(rec.station_id);
                    }
                }
            }
        });

        t.start();
    }

    private void write_parquet(long station_id, ArrayList<GenericRecord> recs) {

        //synchronized (writeParqLock) {
            Schema schema = getSchema();
            if (recordsNum.get(station_id) == null) recordsNum.put(station_id, 0L);
            String stationPath = this.path + File.separator + station_id + File.separator + "sample" + (recordsNum.get(station_id) + 1);
            try (ParquetWriter<GenericRecord> pWriter = AvroParquetWriter
                    .<GenericRecord>builder(HadoopOutputFile.fromPath(new Path(stationPath), new Configuration()))
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {
                for (GenericRecord rec : recs) {
                    pWriter.write(rec);
                }
                recordsNum.put(station_id, recordsNum.get(station_id) + 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("buffer succesfully written to parquet");
        //}
    }

    private Schema getSchema(){
        String recordSchemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Record\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"station_id\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"s_no\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"battery_status\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"status_timestamp\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"weather\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"record\",\n" +
                "        \"name\": \"Weather\",\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"name\": \"humidity\",\n" +
                "            \"type\": \"int\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"temperature\",\n" +
                "            \"type\": \"int\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"wind_speed\",\n" +
                "            \"type\": \"int\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(recordSchemaJson);
        return schema;
    }

//    private GenericRecord createRecord(Record rec){
//        GenericRecord record = new GenericData.Record(Schema.create(Schema.Type.RECORD));
//        record.put("station_id", rec.station_id);
//        record.put("s_no", rec.s_no);
//        record.put("battery_status", rec.battery_status);
//        record.put("status_timestamp", rec.status_timestamp);
//        record.put("weather_humidity", rec.weather.humidity);
//        record.put("weather_temperature", rec.weather.temperature);
//        record.put("weather_wind_speed", rec.weather.wind_speed);
//        return record;
//    }

    private GenericRecord createRecord(Record rec){
        Schema weatherSchema = SchemaBuilder.record("Weather")
                .fields()
                .name("humidity").type().intType().noDefault()
                .name("temperature").type().intType().noDefault()
                .name("wind_speed").type().intType().noDefault()
                .endRecord();

        Schema reocrdSchema = SchemaBuilder.record("Record")
                .fields()
                .name("station_id").type().longType().noDefault()
                .name("s_no").type().longType().noDefault()
                .name("battery_status").type().stringType().noDefault()
                .name("status_timestamp").type().longType().noDefault()
                .name("weather").type(weatherSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(reocrdSchema);
        record.put("station_id", rec.station_id);
        record.put("s_no", rec.s_no);
        record.put("battery_status", rec.battery_status);
        record.put("status_timestamp", rec.status_timestamp);

        GenericRecord weatherRecord = new GenericData.Record(weatherSchema);
        weatherRecord.put("humidity", rec.weather.humidity);
        weatherRecord.put("temperature", rec.weather.temperature);
        weatherRecord.put("wind_speed", rec.weather.wind_speed);

        record.put("weather", weatherRecord);

        return record;
    }


    public static void main(String[] args){
        Parquet p = new Parquet();
        for(int i = 0;i < 60005; i++){
            if (i%1000 == 0) System.out.println(i);
            Weather weather = new Weather(50,60,70);
            Record rec = new Record(i%3, i ,"low",System.currentTimeMillis(),weather);
            p.handle_rec(rec);
        }
    }
}
