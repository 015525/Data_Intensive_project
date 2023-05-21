package org.example.bitcask;

import java.io.File;

public class Paths {
    // / for linux and \\ for windows
    static String split = "\\";
    public static String getDirectory(){
        String path =  String.format("%s%s%s", "bitcask",split, "data");
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
    public static String getHintFilePath(String currentDir, int number) {
        String currentCompactionPath = String.format("%s%s%s%s", currentDir,split, "hint", number);
        System.out.println("Compact path: "+ currentCompactionPath);
        return currentCompactionPath;
    }

    public static String getCurrentFilePath(int lastFileNumber){
        String currentFilePath = String.format("%s%s%s", getDirectory(),split, lastFileNumber);
        System.out.println("CurrentFilePath: " + currentFilePath);
        return currentFilePath;
    }
    public static String getCompactionPath(String currentDir, int number){
        String currentCompactionPath = String.format("%s%s%s%s", currentDir,split, "compacted", number);
        System.out.println("Compact path: "+ currentCompactionPath);
        return currentCompactionPath;
    }
//    public static String lastFileNumber


}
