package org.example.bitcask;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

import static org.example.bitcask.Paths.getDirectory;

public class Recovery {
    HashMap<Long,activeHashedValue> recoverdHashMap = new HashMap<>();
    HashMap<Long,Long> recoveredHashMapTimeStamps = new HashMap<>();

    int NumberOf_Non_CompactedFiles = 0;
    int LastNonCompacted = 0;
    int NumberOfCompactedFiles = 0;

    public void recover(){
        String currentDir = getDirectory();
        File[] files = getFileListSorted(currentDir);
        constructHashMap(files);
        for(long n : recoverdHashMap.keySet()){
            System.out.println("id:  " + n + "  value  " + recoverdHashMap.get(n));
        }

    }

    private void constructHashMap(File[] files) {

        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    if(file.getName().contains("hint")){
                        readHintFile(file.getPath());
                        NumberOfCompactedFiles++;
                    }else if(!file.getName().contains("compacted")){
                        readRegularFile(file.getPath());
                        LastNonCompacted = Math.max(LastNonCompacted, Integer.parseInt(file.getName()));
                        NumberOf_Non_CompactedFiles++;
                    }
                }
            }
        }
    }

    private void readHintFile(String filePath) {
        String compactedPath = getCompactedPath(filePath);
        try (FileInputStream fi = new FileInputStream(filePath);
             DataInputStream is = new DataInputStream(fi)) {
            while (true) {
                try {
                    long time = is.readLong();
                    long id = is.readLong();
                    long offset = is.readLong();
                    if (recoverdHashMap.containsKey(id)) {
                        if (recoveredHashMapTimeStamps.get(id) < time) {
                            recoverdHashMap.put(id, new activeHashedValue(compactedPath, offset));
                            recoveredHashMapTimeStamps.put(id, time);
                        }
                    } else {
                        recoverdHashMap.put(id, new activeHashedValue(compactedPath, offset));
                        recoveredHashMapTimeStamps.put(id, time);
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

    private String getCompactedPath(String hintFile) {
        return  hintFile.replace("hint", "compacted");
    }

    private void readRegularFile(String filePath){
        try (FileInputStream fi = new FileInputStream(filePath);
             DataInputStream is = new DataInputStream(fi)) {
            while (true) {
                try {
                    long offset = fi.getChannel().position();
                    Record rec = new Record();
                    rec.station_id = is.readLong();
                    rec.s_no = is.readLong();
                    rec.battery_status = is.readUTF();
                    rec.status_timestamp = is.readLong();
                    Weather w = new Weather();
                    w.humidity = is.readInt();
                    w.temperature = is.readInt();
                    w.wind_speed = is.readInt();
                    rec.weather = w;
                    if (recoverdHashMap.containsKey(rec.station_id)) {
                        if (recoveredHashMapTimeStamps.get(rec.station_id) < rec.status_timestamp) {
                            recoverdHashMap.put(rec.station_id, new activeHashedValue(filePath, offset));
                            recoveredHashMapTimeStamps.put(rec.station_id, rec.status_timestamp);
                        }
                    } else {
                        recoverdHashMap.put(rec.station_id, new activeHashedValue(filePath, offset));
                        recoveredHashMapTimeStamps.put(rec.station_id, rec.status_timestamp);
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

    private File[] getFileListSorted(String directoryPath){
        File directory = new File(directoryPath);
        File[] files = directory.listFiles();

        if (files != null) {
            // Sort files by last modified timestamp in descending order
            Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());

            // Print the sorted files
            for (File file : files) {
                System.out.println(file.getName() + " - Last Modified: " + file.lastModified());
            }
        }
        return files;
    }

}
