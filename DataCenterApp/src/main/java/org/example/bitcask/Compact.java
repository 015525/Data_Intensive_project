package org.example.bitcask;

import java.io.*;
import java.util.*;

import static org.example.bitcask.Paths.*;

record compactedHashedValue(Record record, long offset) {
}


public class Compact {
    public HashMap<Long, compactedHashedValue> compactedHashMap;
    public List<File> inActiveFiles;
    private Object readLock = new Object();

    private List<File> getListOfInactiveFilesSorted(String currentDir){
        File directory = new File(currentDir);
        File[] files = directory.listFiles();
        assert files != null;
        Arrays.sort(files, (file1, file2) -> {
            long lastModified1 = file1.lastModified();
            long lastModified2 = file2.lastModified();
            return Long.compare(lastModified2, lastModified1);
        });

        ArrayList<File> filteredFiles =  new ArrayList<>(Arrays.asList(files));
//        for (File file : filteredFiles) {
//            System.out.println(file.getName());
//        }
        /////////////////////
        ///// Delete active file
        /////////////////////

        filteredFiles.remove(0);
        return filteredFiles;
    }

    private void readInactiveFiles(){
        synchronized (readLock) {
            compactedHashMap = new HashMap<>();
            String currentDir = getDirectory();
            inActiveFiles = getListOfInactiveFilesSorted(currentDir);
            for (File file : inActiveFiles) {
                if(file.getName().contains("hint")) continue;
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
            }
        }
    }

    private void generateCompactedFile(String currentDir){
        /*
            After I make the compacted hash map I write the compacted file
        */
        String compactionPath = getCompactionPath(currentDir);
        String hintFilePath = getHintFilePath(currentDir);
        try {
            FileOutputStream fo = new FileOutputStream(compactionPath, false);
            DataOutputStream os = new DataOutputStream(fo);
            FileOutputStream foHint = new FileOutputStream(hintFilePath, false);
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

    public void compact(String currentDir){
        readInactiveFiles();
        generateCompactedFile(currentDir);
        System.out.println("Generated compacted file");
    }
    public void collect_old_files() {
        for (File file : inActiveFiles) {
            if(file.getName().contains("hint") || file.getName().contains("compacted")) continue;
            boolean isDeleted = file.delete();
            if (isDeleted) {
                System.out.println("Deleted file: " + file.getName());
            } else {
                System.out.println("Failed to delete file: " + file.getName());
            }
        }
    }
}
