package org.example.bitcask;

import java.io.*;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

import static org.example.bitcask.Paths.*;

record activeHashedValue(String filePath, long offset) {
}
record compactedHashedValue(Record record, long offset) {
}


public class BitCask {
    /* Important notes:
       New day => reset variables
       such as
       lastFileNumber
       lastCompactNumber
       what else should we do?

       When the system goes down?
       these values will reset, but they shouldn't,
       so we need to get the right values for them
    */
    HashMap<Long, activeHashedValue> hashTable = new HashMap<>();
    // To keep track of data files
    int lastFileNumber = 0;
    // To keep track of compacted files
    int lastCompactedNumber = 0;
    private final static int maxFileSize= 1000;
    private final static int FilesLimit = 5;
    private static BitCask bitCask;
    private BitCask() {
    }

    public static BitCask getInstance() {
        if (bitCask == null) {
            synchronized (BitCask.class) {
                if (bitCask == null) {
                    bitCask = new BitCask();
                }
            }
        }
        return bitCask;
    }
    private void createNewFile(){
        lastFileNumber++;
    }
    private void put(Record record, String filepath) {
        long key = record.station_id;
        File file = new File(filepath);
        long offset = file.length();
        this.hashTable.put(key, new activeHashedValue(filepath,offset));
        this.writeObj(record, filepath);
        if (file.length() >= maxFileSize){
            createNewFile();
        }
    }

    private void writeObj(Record record, String filepath){
        try {
            FileOutputStream fo = new FileOutputStream(filepath, true);
            ObjectOutputStream os = new ObjectOutputStream(fo);
//            os.writeLong(record.status_timestamp);
//            os.writeLong(record.station_id);
//            os.writeLong(record);
            os.writeLong(record.station_id);
            os.writeObject(record);
            os.close();
            fo.close();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    private Record get(long key) {
        long offset = this.hashTable.get(key).offset();
        String filepath = this.hashTable.get(key).filePath();
        Record rec = null;
        File file = new File(filepath);
        if (file.exists()) {
            try {
                FileInputStream fi = new FileInputStream(filepath);
                fi.skip(offset);
                ObjectInputStream is = new ObjectInputStream(fi);
                long id = is.readLong();
                rec = (Record) is.readObject();
                is.close();
                fi.close();

            } catch (ClassNotFoundException | IOException e) {
                throw new RuntimeException(e);
            }
        }else{
            System.out.println("Get: File doesn't exist");
        }

        return rec;
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

    // last file number could be modified by another thread
    // this is why it's passed here would this help??


    /*
    I brought all code for reading, writing files
    To avoid threads.
     */
    private void compact(String currentDir, int last) {

//        //////////////////
//        ///This should be done carefully
          // I think old files shouldn't be collected immediately,
          // After compaction this should be done safely;
//        collect_old_files(currentDir);

        //////////////////////.
        HashMap<Long, compactedHashedValue> compactedHashMap = new HashMap<>();
        // for all non-active files
        for(int i = 0; i < last;i++){
            String filepath = String.format("%s%s%s", currentDir,split, i);
            File file = new File(filepath);
            if (file.exists()) {
                try {
                    FileInputStream fi = new FileInputStream(filepath);
                    ObjectInputStream is = new ObjectInputStream(fi);
                    /////offset???????????????????????
                    //// NOT needed
                    //// the file will be deleted any way
                    //////////////////////////////
                    long id = is.readLong();
                    Record rec = (Record) is.readObject();
                    compactedHashMap.put(id, new compactedHashedValue(rec, 0));
                    is.close();
                    fi.close();
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException(e);
                }
            }else{
                System.out.println("Get: File doesn't exist");
            }
        }

        /*
            After I make the compacted hash map I write the compacted file
        */
        String compactionPath = getCompactionPath(currentDir);
        try {
            FileOutputStream fo = new FileOutputStream(compactionPath, true);
            ObjectOutputStream os = new ObjectOutputStream(fo);
            for (long key : compactedHashMap.keySet()) {
                compactedHashedValue value = compactedHashMap.get(key);
                System.out.println("Key: " + key + ", Value: " + value);
                File file = new File(compactionPath);
                long fileLength = file.length();
                value = new compactedHashedValue(value.record(), fileLength);
                System.out.println("Monitor offset: " + value);
                os.writeLong(value.record().station_id);
                os.writeObject(value.record());
            }
            os.close();
            fo.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


        /*
        Updating the active hashmap this should be done carefully
        This code must go somewhere else
        I think here we must stop other readers and writers to
        Modify the active hashmap
        */
        /*
         if renaming that is suggested in the commend below in the same function is
         accepted. Renaming should be done before updating the active hashmap
         */

        for (long key : compactedHashMap.keySet()) {
            compactedHashedValue value = compactedHashMap.get(key);
            if(get(key).status_timestamp == value.record().status_timestamp){
                hashTable.put(key, new activeHashedValue(compactionPath, value.offset()));
            }
        }

//        /////////////////// Don't forget
//        // this should be done carefully
         /*
            Here a problem will arise
            the current file number for example 6
            after setting the value of lastFileNumber to 0
            will be dumped so I think the currentFileName should be
            renamed or something
          */
//        lastFileNumber = 0;
//        lastCompactedNumber++;
//        ///////////////////


        /*
        The compacted file can be compacted too,
        so to make the process easy we can
        name the compacted file with 0 and
        the current active file to 1 and set lastFileNumber to 1
        what do you think???
        Here also we must stop others
         */
    }

    private boolean checkCompact(){
        return lastFileNumber >= FilesLimit;
    }

    private void collect_old_files(String Dir) {
       for(int i=0;i<=lastFileNumber;i++){
           String oldFilePath = String.format("%s%s%s", Dir,split, i);
           File file = new File(oldFilePath);
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
    }
    public void handleMessage(Record rec) {
        String currentFilePath = getCurrentFilePath(rec.status_timestamp, lastFileNumber);
        this.put(rec, currentFilePath);

        // For testing
        Record recG = this.get(1);
        System.out.println(recG);
        System.out.println(this.hashTable.toString());
        //////////////////
        //////////////////
        // we can instead schedule a thread that runs every x seconds????????
        if(checkCompact()){
            this.compact(getDirectory(rec.status_timestamp), lastFileNumber);
        }
        //////////////////
        /////////////////
    }
}
