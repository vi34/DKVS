package com.vi34;


import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Created by vi34 on 16/06/16.
 */
public class MyLogger {

    BufferedWriter writer;

    MyLogger(Path file) {
        try {
            if (Files.notExists(file)) {
                Files.createFile(file);
            }
            writer = Files.newBufferedWriter(file, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void info (String s) {
        try {
            writer.write(s+ "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
