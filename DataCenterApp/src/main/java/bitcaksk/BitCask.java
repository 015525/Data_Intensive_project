package bitcaksk;

import java.io.*;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class BitCask {
    HashMap<Long, Long> hashTable = new HashMap<Long, Long>();
    String filepath = "E:\\year3_term2\\Data_Intensive\\project\\database\\data.txt";
    String compactionpath = "E:\\year3_term2\\Data_Intensive\\project\\campaction\\dataComp.txt";
    String oldfilepath = "E:\\year3_term2\\Data_Intensive\\project\\campaction\\data.txt";

    private void put(Record record) throws FileNotFoundException {
        long key = record.station_id;
        File file = new File(filepath);
        long offset = file.length();

        this.hashTable.put(key, offset);
        this.writeObj(record, this.filepath);
    }

    private void writeObj(Record record, String filepath){
        try {
            FileOutputStream fo = new FileOutputStream(filepath, true);
            ObjectOutputStream os = new ObjectOutputStream(fo);

            os.writeObject(record);

            os.close();
            fo.close();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Record get(long key) {
        long offset = this.hashTable.get(key);
        Record rec = new Record();
        try {
            FileInputStream fi = new FileInputStream(this.filepath);
            fi.skip(offset);
            ObjectInputStream is = new ObjectInputStream(fi);


            rec = (Record) is.readObject();

            is.close();
            fi.close();

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return rec;
    }

    private void compact() throws FileNotFoundException {
        this.collect_old_file();
        for (long key :hashTable.keySet()){
            Record rec = this.get(key);
            this.writeObj(rec, this.compactionpath);
        }
    }

    private void collect_old_file() {
        File file = new File(this.oldfilepath);
        if (file.exists()){
            if (file.delete()){
                System.out.println("file deleted  successfully");
            }else {
                System.out.println("failed to delete file");
            }
        }else{
            System.out.println("File doesn't exists");
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        Weather weather = new Weather(50,60,70);
        Record rec = new Record(1,1,"low",978,weather);

        BitCask bc = new BitCask();
        bc.put(rec);
        Record recG = bc.get(1);
        System.out.println(recG.toString());
        System.out.println(bc.hashTable.toString());
        bc.compact();
    }
}
