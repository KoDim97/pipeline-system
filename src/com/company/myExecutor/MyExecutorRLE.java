package com.company.myExecutor;

import java.io.*;
import java.util.*;

import com.company.pipeline.Writer;
import ru.spbstu.pipeline.*;

public class MyExecutorRLE implements ru.spbstu.pipeline.Executor {
    private Map<Producer, DataAccessor> prod_access = new HashMap<>();
    private Consumer consumer;
    private Producer producer;
    private Status status;
    private final ru.spbstu.pipeline.logging.Logger logger;
    private int block_size;
    private Object returnObject;
    private byte[] output;
    private byte[] input;
    private static final String GRAMMAR_SEPARATOR = "=";
    private MODE curMode;
    ERRORS errors;
    enum MODE {
        COMPRESS, DECOMPRESS;
    }
    enum ERRORS{
        NO_ERRORS(0, "Everything is OK!"),
        READ_CONFIG_FILE(1, "Can not read config.txt"),
        CONFIG_MODE(2, "Can not find mode grammar in config.txt."),
        CONFIG_MODE_TYPE(3, "Can not accept chosen mode."),
        CONFIG_DELIMITER(4, "Can not find valid delimiter symbol."),
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

    static final String NAME_OF_LOG_FILE = "log.txt";

    enum GRAMMAR_PARAM {
        MODE,
        PARSE_BY
    }
    enum GRAMMAR_MODES{
        COMPRESS,
        DECOMPRESS
    }

    public ERRORS Process(RLE rle){
        if (curMode == MODE.COMPRESS){
            output = rle.toPrimitives(rle.compress(input));
        }
        else if (curMode == MODE.DECOMPRESS){
            output = rle.toPrimitives(rle.decompress(input));
        }
        return ERRORS.NO_ERRORS;
    }


    public MyExecutorRLE(String configName, ru.spbstu.pipeline.logging.Logger logger) throws IOException {
        this.logger = logger;
        errors = parseConf(configName);
        if(errors != ERRORS.NO_ERRORS) {
            logger.log(errors.getMessage());
            status = Status.ERROR;
        }
        status = Status.OK;
    }

    ERRORS parseConf(String config) {
        String configContent;
        try{
            File file = new File(config);
            configContent = new String(readFile(file));
        }
        catch (IOException ex){
            return ERRORS.READ_CONFIG_FILE;
        }
        int pos, nextLine;
        String curParamValue;

        for (GRAMMAR_PARAM curGramma : GRAMMAR_PARAM.values()) {
            pos = configContent.lastIndexOf(curGramma.name());
            if (pos == -1) {
                switch (curGramma.ordinal()) {
                    case 0:
                        return ERRORS.CONFIG_MODE;
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
                    if (curParamValue.equals(GRAMMAR_MODES.COMPRESS.name())) {
                        curMode = MODE.COMPRESS;
                    }
                    else if (curParamValue.equals(GRAMMAR_MODES.DECOMPRESS.name())){
                        curMode = MODE.DECOMPRESS;
                    }
                    else {
                        return ERRORS.CONFIG_MODE_TYPE;
                    }
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
        return ERRORS.NO_ERRORS;
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
    public void run() {
        if (status != Status.OK){
            return;
        }
        logger.log("MyExecutorRLE started");
        RLE rle = new RLE();
        Process(rle);
        logger.log("MyExecutorRLE finished");
        if (consumer.loadDataFrom(this) != 0){
            consumer.run();
        }else {
            status = Status.EXECUTOR_ERROR;
            return;
        }
        this.status = consumer.status();
    }

    @Override
    public long loadDataFrom(Producer prod){
        DataAccessor accessor = prod_access.get(prod);
        //long buffSize = accessor.size();
        input = (byte[])accessor.get();
        return input.length;
    }

    @Override
    public void addConsumer(Consumer cons){
        consumer = cons;
    }

    @Override
    public void addConsumers(List<Consumer> consList)
    {
        addConsumer(consList.get(0));
    }

    @Override
    public void addProducer(Producer prod){
        Set<String> available = prod.outputDataTypes();
        if (!available.contains(byte[].class.getCanonicalName())){
            status = Status.EXECUTOR_ERROR;
            logger.log("No byte[] output from producer " +
                    producer.getClass().getCanonicalName());
        }
        prod_access.put(prod, prod.getAccessor(byte[].class.getCanonicalName()));
        producer = prod;
    }

    @Override
    public void addProducers(List<Producer> prodList){
        addProducer(prodList.get(0));
    }
    @Override
    public Status status() {
        return status;
    }
    private static String msg(String msg) {
        return Writer.class.getName() + " " + msg;
    }

    @Override
    public Set<String> outputDataTypes() {
        Set<String> dt = new HashSet<String>();
        dt.add(String.class.getCanonicalName());
        dt.add(char[].class.getCanonicalName());
        dt.add(byte[].class.getCanonicalName());
        return dt;
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
