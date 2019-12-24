package com.company.pipeline;

import ru.spbstu.pipeline.*;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Manager {
    enum MAIN_CONFIG_GRAMMAR {
        LOG_FILE,
        READER_CFG,
        EXECUTORS_FILE,
        WRITER_CFG
    }
    enum EXECUTORS_CONFIG_GRAMMAR {
        EXECUTOR_NAME,
        EXECUTOR_CFG
    }
    enum ERRORS{
        NO_ERRORS(0, "Everything is OK!"),
        READ_CONFIG_FILE(1, "Can not read config"),
        CONFIG_STRUCTURE(2, "Config must contains pairs PARAM = VALUE"),
        CONFIG_EXECUTOR_CFG(3, "Can not find config for executor."),
        CONFIG_LOG_FILE(4, "Can not find log file grammar in config"),
        CONFIG_READER_FILE(5, "Can not find reader grammar in config"),
        CONFIG_WRITER_FILE(6, "Can not find writer grammar in config"),
        CONFIG_EXECUTORS_FILE(7, "Can not find executors in config"),
        CONFIG_DELIMITER(8, "Can not find valid delimiter symbol.");

        private final int id;
        private final String message;
        ERRORS(int id, String message){
            this.id = id;
            this.message = message;
        }
        public int getId() {return id;}
        public String getMessage(){return message;}
    }
    private static final String GRAMMAR_SEPARATOR = "=";
    private ArrayList<Executor> exs;
    private String logFileName;
    private String readerCfgName;
    private String writerCfgName;
    private String executorsCfgName;
    private ERRORS errors = ERRORS.NO_ERRORS;
    private Logger logger;
    private Reader reader;
    private Writer writer;

    Manager(String configName){
        try{
            exs = new ArrayList<Executor>();
            Map<String, String> executorsData = new LinkedHashMap<String, String>();
            errors = ParseConfig(configName);
            if (errors == ERRORS.NO_ERRORS){
                logger = new Logger();
                logger.addFile(logFileName);
                reader = new Reader(readerCfgName, logger);
                writer = new Writer(writerCfgName, logger);
                errors = ParseExecutors(executorsCfgName, executorsData);
                if (errors != ERRORS.NO_ERRORS){
                    logger.log(errors.getMessage());
                }
                CreatingPipeline(executorsData);
            }
        }
        catch (IOException ex){
            errors = ERRORS.READ_CONFIG_FILE;
        }
    }
    private ERRORS ParseExecutors(String executorsFile, Map<String, String> executors) throws IOException {
        BufferedReader configReader = new BufferedReader(new FileReader(executorsFile));
        String line;
        while ((line = configReader.readLine()) != null) {
            String[] words = line.split(GRAMMAR_SEPARATOR);
            if (words.length != 2){
                return ERRORS.CONFIG_STRUCTURE;
            }
            if (words[0].contains(EXECUTORS_CONFIG_GRAMMAR.EXECUTOR_NAME.name())) {
                String curName = words[1].trim();
                line = configReader.readLine();
                words = line.split(GRAMMAR_SEPARATOR);
                if (words.length != 2) {
                    return ERRORS.CONFIG_STRUCTURE;
                }
                if (words[0].contains(EXECUTORS_CONFIG_GRAMMAR.EXECUTOR_CFG.name())) {
                    executors.put(words[1].trim(), curName);
                }
                else {
                    return ERRORS.CONFIG_EXECUTOR_CFG;
                }
            }
        }
        return ERRORS.NO_ERRORS;
    }
    private ERRORS ParseConfig(String configName) throws IOException {
        String configContent;
        File file = new File(configName);
        configContent = new String(readFile(file));
        int pos, nextLine;
        String curParamValue;
        for (MAIN_CONFIG_GRAMMAR curGramma : MAIN_CONFIG_GRAMMAR.values()) {
            pos = configContent.lastIndexOf(curGramma.name());
            if (pos == -1) {
                switch (curGramma.ordinal()){
                    case 0:
                        return ERRORS.CONFIG_LOG_FILE;
                    case 1:
                        return ERRORS.CONFIG_READER_FILE;
                    case 2:
                        return ERRORS.CONFIG_EXECUTORS_FILE;
                    case 3:
                        return ERRORS.CONFIG_WRITER_FILE;
                }
            }
            pos = configContent.indexOf(GRAMMAR_SEPARATOR, pos);
            if (pos == -1) {
                return ERRORS.CONFIG_DELIMITER;
            }
            nextLine = configContent.indexOf('\r', pos);
            if (nextLine == -1) {
                nextLine = configContent.length();
            }
            curParamValue = configContent.substring(pos + GRAMMAR_SEPARATOR.length(), nextLine);
            curParamValue = curParamValue.trim();
            switch (curGramma.ordinal()) {
                case 0:
                    logFileName = curParamValue;
                    break;
                case 1:
                    readerCfgName = curParamValue;
                    break;
                case 2:
                    executorsCfgName = curParamValue;
                    break;
                case 3:
                    writerCfgName = curParamValue;
                default:
                    break;
            }
        }
        if (writerCfgName == null || executorsCfgName == null || readerCfgName == null || logFileName == null){
            return ERRORS.CONFIG_STRUCTURE;
        }
        return ERRORS.NO_ERRORS;
    }
    private void CreatingPipeline(Map<String, String> executors){
        for (Map.Entry<String, String> entry: executors.entrySet()) {
           try {
               Class obj = Class.forName(entry.getValue().trim());
               Class[] construct = {String.class, ru.spbstu.pipeline.logging.Logger.class};
               Executor curExecutor = (Executor)obj.getConstructor(construct).newInstance(entry.getKey(), logger);
               exs.add(curExecutor);
           }
           catch (ClassNotFoundException ex){

           }
           catch (IllegalAccessException ex){

           }
           catch (java.lang.InstantiationException ex){

           }
           catch (NoSuchMethodException e) {

           }
           catch (InvocationTargetException e) {

           }
        }
        reader.addConsumer(exs.get(0));
        exs.get(0).addProducer(reader);
        exs.get(0).addConsumer(exs.get(1));
        for (int i = 1; i < exs.size() - 1; i++) {
            exs.get(i).addProducer(exs.get(i - 1));
            exs.get(i).addConsumer(exs.get(i + 1));
        }
        exs.get(exs.size() - 1).addProducer(exs.get(exs.size() - 2));
        exs.get(exs.size() - 1).addConsumer(writer);
        writer.addProducer(exs.get(exs.size() - 1));
    }
    public void Run(){
        Map<Consumer, Thread> map = new HashMap<>();
        reader.run();
    }
    public ERRORS getErrors() {
        return errors;
    }
    private byte[] readFile (File file) throws IOException{
        FileInputStream fileInputStream = null;
        byte[] bFile = new byte[(int) file.length()];
        fileInputStream = new FileInputStream(file);
        fileInputStream.read(bFile);
        fileInputStream.close();
        return bFile;
    }
}
