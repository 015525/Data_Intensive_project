package org.example.bitcask;

import java.io.File;
import java.time.LocalDateTime;

public class Paths {
    // / for linux and \\ for windows
    static String split = "/";
    public static String getDirectory(long timestamp){
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(timestamp / 1000, 0, java.time.ZoneOffset.UTC);
        String path =  String.format("%s%s%s%s%s", dateTime.getYear(),split, dateTime.getMonthValue(),split,dateTime.getDayOfMonth());
        System.out.println("Current path: " + path);
        //////////////////////
        createDirectoryPath(path);
        //////////////////////
        return path;
    }

    public static void createDirectoryPath(String path){
        File multiLevelDirectory = new File(path);
        boolean isMultiLevelCreated = multiLevelDirectory.mkdirs();
        if (isMultiLevelCreated) {
            System.out.println("Folder path created successfully.");
        } else if (multiLevelDirectory.exists()) {
            System.out.println("Folder path already exists.");
        } else {
            System.out.println("Failed to create the folder path.");
        }
    }

    public static String getCurrentFilePath(long status_timestamp, int lastFileNumber){
        String currentFilePath = String.format("%s%s%s", getDirectory(status_timestamp),split, lastFileNumber);
        System.out.println("CurrentFilePath: " + currentFilePath);
        return currentFilePath;
    }
    public static String getCompactionPath(String currentDir){
        String currentCompactionPath = String.format("%s%s%s", currentDir,split, "compacted");
        System.out.println("Compact path: "+ currentCompactionPath);
        return currentCompactionPath;
    }
//    public static String lastFileNumber


}
