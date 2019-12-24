package com.company.pipeline;
import ru.spbstu.pipeline.*;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class Reader implements ru.spbstu.pipeline.Reader {
    enum READER_CONFIG_GRAMMAR {
        INPUT_FILE,
        PARSE_BY
    }
    enum ERRORS{
        NO_ERRORS(0, "Everything is OK!"),
        READ_CONFIG_FILE(2, "Can not read config"),
        CONFIG_INPUT_FILE(3, "Can not find input file grammar in config"),
        OPEN_INPUT_FILE(4, "Can not open input file"),
        CONFIG_STRUCTURE(5, "Config must contains pairs PARAM = VALUE"),
        CONFIG_DELIMITER(6, "Can not find valid delimiter symbol."),
        CONFIG_PARSE_BY(7, "Can not find parse by grammar in config.txt."),
        CONFIG_PARSE_BY_VALUE(8, "Invalid value of PARSE_BY parameter.");

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

    private InputStream is;
    private String inputFileName;
    private int block_size;
    private final Logger logger;
    
    private byte[] output;
    private Object returnObject;
    private Consumer consumer;
    private Status status;

    public Reader(String configName, Logger logger) {
        this.logger = logger;
        ERRORS errors;
        try{
             errors = parseReaderCfg(configName);
             if (errors != ERRORS.NO_ERRORS){
                 logger.log(errors.getMessage());
                 status = Status.ERROR;
             }
            try{
                is = new FileInputStream(inputFileName);
            }
            catch (FileNotFoundException ex){
                errors = ERRORS.OPEN_INPUT_FILE;
                logger.log(errors.getMessage());
                status = Status.ERROR;
            }
        }
        catch (IOException ex){
            errors = ERRORS.READ_CONFIG_FILE;
            logger.log(errors.getMessage());
            status = Status.ERROR;
        }
    }
    private ERRORS parseReaderCfg(String configName) throws IOException{
        String configContent;
        File file = new File(configName);
        configContent = new String(readFile(file));
        int pos, nextLine;
        String curParamValue;
        for (READER_CONFIG_GRAMMAR curGramma : READER_CONFIG_GRAMMAR.values()) {
            pos = configContent.lastIndexOf(curGramma.name());
            if (pos == -1) {
                switch (curGramma.ordinal()){
                    case 0:
                        return ERRORS.CONFIG_INPUT_FILE;
                    case 1:
                        return ERRORS.CONFIG_PARSE_BY;
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
                    inputFileName = curParamValue;
                    break;
                case 1:
                    try{
                        block_size = Integer.parseInt(curParamValue);
                    }
                    catch (NumberFormatException ex){
                        return ERRORS.CONFIG_PARSE_BY_VALUE;
                    }
                    break;
            }
        }
        if (block_size == 0 || inputFileName == null){
            return ERRORS.CONFIG_STRUCTURE;
        }
        return ERRORS.NO_ERRORS;
    }

    @Override
    public void run(Map<Consumer, Thread> map) {
        status = Status.OK;
        try {
            loop(map);
        }
        catch (IOException e) {
            status = Status.READER_ERROR;
            logger.log(Level.SEVERE, msg("reading error"), e);
        }
    }

    class ConcreteDataAccessor implements DataAccessor {

        ConcreteDataAccessor( String s ) {
            typeName = s;
        }

        @Override
        public Object get() {
            if (typeName.equals(char[].class.getCanonicalName())) {
                try {
                    returnObject = new String(output, "UTF-16BE").toCharArray();
                } catch (UnsupportedEncodingException e) {
                    logger.log(e.getMessage());
                }
            }
            else if (typeName.equals(byte[].class.getCanonicalName())) {
                returnObject = output.clone();
            }
            else if (typeName.equals(String.class.getCanonicalName())) {
                try {
                    returnObject = new String(output, "UTF-16BE");
                } catch (UnsupportedEncodingException e) {
                    logger.log(e.getMessage());
                }
            }
            else
                logger.log("Not supported type");

            return returnObject;
        }

        @Override
        public long size() {
            if (typeName.equals(byte[].class.getCanonicalName()))
                return ((byte[])returnObject).length;
            if (typeName.equals(char[].class.getCanonicalName()))
                return ((char[])returnObject).length;
            if (typeName.equals(String.class.getCanonicalName()))
                return ((String)returnObject).length();
            logger.log("Not supported type");
            return 0;
        }

        String typeName;
    }

    @Override
    public DataAccessor getAccessor( String s ) {
        return new ConcreteDataAccessor(s);
    }

    @Override
    public void addConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void addConsumers(List<Consumer> consumers) {
        addConsumer(consumers.get(0));
    }

    private void loop(Map<Consumer, Thread> map) throws IOException {
        int rc = 0;
        do {
            byte[] buffer = new byte[block_size];
            rc = is.read(buffer);
            if (rc < block_size && rc > 0){
                output = new byte[rc];
                System.arraycopy(buffer, 0, output, 0, rc);
            }
            else {
                output = buffer;
            }
            consumer.loadDataFrom(this);
            if (consumer.status() != Status.OK){
                status = Status.EXECUTOR_ERROR;
                return;
            }
            consumer.run();
        }while(rc == block_size);
    }


    @Override
    public Set<String> outputDataTypes() {
        Set<String> dt = new HashSet<String>();
        dt.add(String.class.getCanonicalName());
        dt.add(char[].class.getCanonicalName());
        dt.add(byte[].class.getCanonicalName());
        return dt;
    }

    private static String msg(String msg) {
        return Reader.class.getName() + " " + msg;
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
