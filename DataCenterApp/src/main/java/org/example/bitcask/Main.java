package org.example.bitcask;

import java.io.File;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class Main {

    public static void main(String[] args) {
        BitCask bitCask = new BitCask();
        Recovery recovery = new Recovery();
        recovery.recover();
        BitCask.hashTable = recovery.recoverdHashMap;
        BitCask.NumberOfCompactedFiles = recovery.NumberOfCompactedFiles;
        BitCask.NumberOf_Non_CompactedFiles = recovery.NumberOf_Non_CompactedFiles;
        BitCask.LastNonCompacted = recovery.LastNonCompacted;
        for(long n : BitCask.hashTable.keySet()){
            System.out.println(bitCask.get(n));
        }
//
//        for(int i = 0;i < 1005; i++){
//            Weather weather = new Weather(50,60,70);
//            Record rec = new Record(i % 10, i ,"low",System.currentTimeMillis(),weather);
//            bitCask.handleMessage(rec);
////            for(long n : BitCask.hashTable.keySet()){
////            System.out.println(bitCask.get(n));
////        }
//        }

    }
}
