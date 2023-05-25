package org.example.bitcask;

import java.awt.desktop.SystemSleepEvent;
import java.io.File;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        BitCask bitCask = new BitCask();
        Parquet parquet = new Parquet();
//        Recovery recovery = new Recovery();
//        recovery.recover();
//        BitCask.hashTable = recovery.recoverdHashMap;
//        BitCask.LastNonCompacted = recovery.LastNonCompacted;
//        for(long n : BitCask.hashTable.keySet()){
//            System.out.println("key is " + n);
//            System.out.println(bitCask.get(n));
//        }
//
//        long startw = System.currentTimeMillis();
////        Thread t =  new Thread(new Runnable() {
////            @Override
////            public void run() {
                for(int i = 0;i < 6000; i++){
                    Weather weather = new Weather(50,60,70);
                    Record rec = new Record(i % 10, i ,"low",System.currentTimeMillis(),weather);
                    try {
                        bitCask.handleMessage(rec);
//                        parquet.handle_rec(rec);

//                        for(long n : BitCask.hashTable.keySet()){
//                            System.out.println(bitCask.get(n));
//                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
//            }
////        });
//        t.start();
//        t.join();
//        long endw = System.currentTimeMillis();
//        System.out.println("Time taken for write 1005 : "+ (endw-startw));
//
//        long startr = System.currentTimeMillis();
        for(long n : BitCask.hashTable.keySet()){
            System.out.println(bitCask.get(n));
        }
//        long endr = System.currentTimeMillis();
//        System.out.println("Time taken to read 10 stations : "+ (endr-startr));
    }
}
