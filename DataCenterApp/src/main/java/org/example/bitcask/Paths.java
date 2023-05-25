package org.example.bitcask;

import java.io.File;

public class Paths {
    // / for linux and \\ for windows
    static String split = "/";
    public static String getDirectory(){
        String path =  String.format("%s%s%s", "bitcask",split, "data");
        createDirectoryPath(path);
        return path;
    }

    public static void createDirectoryPath(String path){
        File multiLevelDirectory = new File(path);
        boolean isMultiLevelCreated = multiLevelDirectory.mkdirs();
    }
    public static String getHintFilePath(String currentDir) {
        return String.format("%s%s%s", currentDir,split, "hint");
    }

    public static String getCurrentFilePath(int lastNonCompacted){
        return String.format("%s%s%s", getDirectory(),split, lastNonCompacted);
    }
    public static String getCompactionPath(String currentDir){
        return String.format("%s%s%s", currentDir,split, "compacted");
    }
}
