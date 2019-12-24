package com.company.pipeline;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

public class Logger implements ru.spbstu.pipeline.logging.Logger {
    private java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Logger.class.getName());
    Logger() {
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);

    }
    public boolean addFile(String ConfigFileName) {
        try {
            FileHandler file_hand = new FileHandler(ConfigFileName);
            SimpleFormatter stor_matter = new SimpleFormatter();
            file_hand.setFormatter(stor_matter);
            logger.addHandler(file_hand);

        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Error of creating log");
            return false;
        } catch (SecurityException ex) {
            logger.log(Level.SEVERE, "Error of creating log");
            return false;
        }
        return true;
    }
    @Override
    public void log(String s) {
        logger.log(Level.SEVERE, s);
    }


    @Override
    public void log(String s, Throwable throwable) {
        logger.log(Level.SEVERE, s,throwable);
    }

    @Override
    public void log(Level level, String s) {
        logger.log(level, s);
    }

    @Override
    public void log(Level level, String s, Throwable throwable) {
        logger.log(level, s,throwable);
    }
}

