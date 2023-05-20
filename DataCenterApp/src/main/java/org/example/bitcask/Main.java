package org.example.bitcask;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.LocalDate;

public class Main {

    public static void main(String args[]) throws FileNotFoundException {
        Weather weather = new Weather(50,60,70);
        Record rec = new Record(1,1,"low",System.currentTimeMillis(),weather);
        BitCask bitCask = BitCask.getInstance();
        bitCask.handleMessage(rec);
    }



}
