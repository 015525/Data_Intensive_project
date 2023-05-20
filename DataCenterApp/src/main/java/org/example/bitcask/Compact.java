package org.example.bitcask;

import java.io.*;
import java.util.*;

import static org.example.bitcask.Paths.*;

record compactedHashedValue(Record record, long offset) {
}


public class Compact {
    public HashMap<Long, compactedHashedValue> compactedHashMap;
    public List<File> inActiveFiles;
    private List<File> getListOfInactiveFilesSorted(String currentDir){
        List<File> filteredFiles = new ArrayList<>();
        File directory = new File(currentDir);
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() ) {
                    try{
                        int fileNameInt = Integer.parseInt(file.getName());
                        if(fileNameInt >= 0){
                            filteredFiles.add(file);
                        }
                    }catch (Exception ignored){
                    }
                }
            }
        }
        filteredFiles.sort((file1, file2) -> {
            int number1 = Integer.parseInt(file1.getName());
            int number2 = Integer.parseInt(file2.getName());
            return Integer.compare(number2, number1);
        });
        /////////////////////
        ///// Delete active file
        /////////////////////
        filteredFiles.remove(0);
        return filteredFiles;
    }
    private void readInactiveFiles(){
        compactedHashMap = new HashMap<>();
        String currentDir = getDirectory();
        inActiveFiles = getListOfInactiveFilesSorted(currentDir);
        for (File file : inActiveFiles) {
            try (FileInputStream fi = new FileInputStream(file);
                 DataInputStream is = new DataInputStream(fi)) {
                while (true) {
                    try {
                        Record rec = new Record();
                        rec.station_id = is.readLong();
                        long id = rec.station_id;
                        rec.s_no = is.readLong();
                        rec.battery_status = is.readUTF();
                        rec.status_timestamp = is.readLong();
                        Weather w = new Weather();
                        w.humidity = is.readInt();
                        w.temperature = is.readInt();
                        w.wind_speed = is.readInt();
                        rec.weather = w;
                        if (compactedHashMap.containsKey(id)) {
                            if (compactedHashMap.get(id).record().status_timestamp < rec.status_timestamp) {
                                compactedHashMap.put(id, new compactedHashedValue(rec, 0));
                            }
                        } else {
                            compactedHashMap.put(id, new compactedHashedValue(rec, 0));
                        }
                    } catch (EOFException e) {
                        fi.close();
                        is.close();
                        break; // Reached end of file
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
                /*
                    No need to continue
                 */
            if (compactedHashMap.keySet().size() == BitCask.NumberOfKeys) {
                break;
            }
        }


    }
    private void generateCompactedFile(String currentDir, int lastCompactedNumber){
        /*
            After I make the compacted hash map I write the compacted file
        */
        String compactionPath = getCompactionPath(currentDir, lastCompactedNumber);
        String hintFilePath = getHintFilePath(currentDir, lastCompactedNumber);
        try {
            FileOutputStream fo = new FileOutputStream(compactionPath, true);
            DataOutputStream os = new DataOutputStream(fo);
            FileOutputStream foHint = new FileOutputStream(hintFilePath, true);
            DataOutputStream osHint = new DataOutputStream(foHint);
            for (long key : compactedHashMap.keySet()) {
                compactedHashedValue value = compactedHashMap.get(key);
                File file = new File(compactionPath);
                long fileLength = file.length();
                value = new compactedHashedValue(value.record(), fileLength);
                Record record = value.record();
                os.writeLong(record.station_id);
                os.writeLong(record.s_no);
                os.writeUTF(record.battery_status);
                os.writeLong(record.status_timestamp);
                os.writeInt(record.weather.humidity);
                os.writeInt(record.weather.temperature);
                os.writeInt(record.weather.wind_speed);
                //////////////HINT/////////////
                osHint.writeLong(record.status_timestamp);
                osHint.writeLong(record.station_id);
                osHint.writeLong(fileLength);
            }
            os.close();
            fo.close();
            osHint.close();
            foHint.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void compact(String currentDir, int NumberOfCompactedFiles){
        readInactiveFiles();
        generateCompactedFile(currentDir, NumberOfCompactedFiles);
    }

    public void collect_old_files() {
        /*
            collect inactive files only
         */
        for (File file : inActiveFiles) {
            boolean isDeleted = file.delete();
            if (isDeleted) {
                System.out.println("Deleted file: " + file.getName());
            } else {
                System.out.println("Failed to delete file: " + file.getName());
            }
        }
        BitCask.NumberOf_Non_CompactedFiles -= inActiveFiles.size();
    }
    public void deleteHintFiles(){

    }
}
