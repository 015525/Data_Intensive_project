package org.example.bitcask;

import java.io.*;
import java.util.HashMap;
import java.io.FileInputStream;
import java.util.concurrent.CountDownLatch;

import static org.example.bitcask.Paths.*;
record activeHashedValue(String filePath, long offset) {
}
public class BitCask {
    /* Important notes:
       When the system goes down?
       these values will reset, but they shouldn't,
       so we need to get the right values for them
       RECOVERY
    */
    static volatile HashMap<Long, activeHashedValue> hashTable = new HashMap<>();
    // To keep track of Non compacted Files
    static volatile int LastNonCompacted = 0;
    // To keep track of compacted files
    private final static int maxFileSize= 1000;
    private static final Compact compactObj = new Compact();
    private Object compactLock = new Object();
    private Object handleMessageLock = new Object();
    private CountDownLatch latch = new CountDownLatch(100);

    public BitCask(CountDownLatch latch) {
        this.latch = latch;
    }

    public BitCask() {}

    private synchronized void createNewFile(){
        LastNonCompacted++;
    }

    private void put(Record record, String filepath) {
        long key = record.station_id;
        File file = new File(filepath);
        long offset = file.length();
        hashTable.put(key, new activeHashedValue(filepath,offset));
        this.writeObj(record, filepath);
        if (file.length() >= maxFileSize){
            createNewFile();
        }
    }

    private void writeObj(Record record, String filepath){
        try {
            FileOutputStream fo = new FileOutputStream(filepath, true);
            DataOutputStream os = new DataOutputStream(fo);
            os.writeLong(record.station_id);
            os.writeLong(record.s_no);
            os.writeUTF(record.battery_status);
            os.writeLong(record.status_timestamp);
            os.writeInt(record.weather.humidity);
            os.writeInt(record.weather.temperature);
            os.writeInt(record.weather.wind_speed);
            os.close();
            fo.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    public Record get(long key) throws InterruptedException {
        Record record = new Record();;
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                long offset = hashTable.get(key).offset();
                String filepath = hashTable.get(key).filePath();
                File file = new File(filepath);
                if (file.exists()) {
                    try {
                        FileInputStream fi = new FileInputStream(filepath);
                        fi.skip(offset);
                        DataInputStream is = new DataInputStream(fi);
                        //record = new Record();
                        record.station_id = is.readLong();
                        record.s_no = is.readLong();
                        record.battery_status = is.readUTF();
                        record.status_timestamp = is.readLong();
                        Weather w = new Weather();
                        w.humidity = is.readInt();
                        w.temperature = is.readInt();
                        w.wind_speed = is.readInt();
                        record.weather = w;
                        is.close();
                        fi.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }else{
                    System.out.println("Get: File doesn't exist");
                }
                //return record;
            }
        });
        t.start();
        t.join();
        return record;
    }

    /*
        background compaction process is spun off which merges
        ((all non-active immutable files)) and produces output files with the latest
        modifications for all the keys. The compaction process also modifies the in-memory KeyDir
        with information about merged files. Now that this compaction is performed
        periodically, we can ensure that the recovery process won’t get bogged down
        due to large number of non-active files, and we will be able to reboot the database
        server in an efficient manner.
     */


    private void compact(String CurrentDir) throws InterruptedException {

        compactObj.compact(CurrentDir);
        HashMap<Long, compactedHashedValue> compactedHashMap = compactObj.compactedHashMap;
        String compactionPath = getCompactionPath(CurrentDir);

        for (long key : compactedHashMap.keySet()) {
            compactedHashedValue value = compactedHashMap.get(key);
            try {
                if (get(key).status_timestamp == value.record().status_timestamp) {
                    hashTable.put(key, new activeHashedValue(compactionPath, value.offset()));
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        ///////////
        // This should be done carefully
        ///////////
        compactObj.collect_old_files();
        //////////
        //////////

    }

    private void checkCompact(){
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (compactLock) {
                    BitCask bt = new BitCask();
                    //System.out.println("count down latch is " + latch.getCount());
                    if (latch.getCount() <= 0) {
                        {
                            try {
                                bt.compact(getDirectory());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            latch = new CountDownLatch(100);
                        }
                    }
                }
            }
        });
        t.start();
    }
    public void handleMessage(Record rec) throws InterruptedException {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (handleMessageLock) {
                    BitCask bt = new BitCask();
                    String currentFilePath = getCurrentFilePath(LastNonCompacted);
                    bt.put(rec, currentFilePath);
                    latch.countDown();
                    checkCompact();

                }
            }
        });
        t.start();
    }
}
