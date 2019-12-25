package com.company.pipeline;

import ru.spbstu.pipeline.*;

import java.io.*;
import java.sql.Array;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class Writer implements ru.spbstu.pipeline.Writer {
    enum WRITER_CONFIG_GRAMMAR {
        OUTPUT_FILE,
        PARSE_BY
    }
    enum ERRORS{
        NO_ERRORS(0, "Everything is OK!"),
        READ_CONFIG_FILE(2, "Can not read config"),
        CONFIG_OUTPUT_FILE(3, "Can not find output file grammar in config"),
        OPEN_OUTPUT_FILE(4, "Can not open output file"),
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

    private final byte[] DONE = {-1};
    private Map<Producer, Producer.DataAccessor> prod_access = new HashMap<>();
    private OutputStream os;
    private final Logger logger;
    private String outputFilename;
    private ArrayList<Producer> producers;
    private BlockingQueue<byte[]> inputQueue;
    private byte[] input;
    private int block_size;
    private Status status;
    private AtomicInteger curThread;
    private AtomicInteger handled;
    private AtomicInteger ended;

    public Writer(String configName, Logger logger) {
        this.logger = logger;
        inputQueue = new ArrayBlockingQueue<>(10, true);
        curThread = new AtomicInteger(0);
        handled = new AtomicInteger(0);
        ended = new AtomicInteger(0);
        producers = new ArrayList<>();
        ERRORS errors;
        try{
            errors = parseWriterCfg(configName);
            if (errors != ERRORS.NO_ERRORS){
                logger.log(errors.getMessage());
                status = Status.ERROR;
            }
            try{
                os = new FileOutputStream(outputFilename);
            }
            catch (FileNotFoundException ex){
                errors = ERRORS.OPEN_OUTPUT_FILE;
                logger.log(errors.getMessage());
                status = Status.ERROR;
            }
        }
        catch (IOException ex){
            errors = ERRORS.READ_CONFIG_FILE;
            logger.log(errors.getMessage());
            status = Status.ERROR;
        }
        status = Status.OK;
    }
    private ERRORS parseWriterCfg(String configName) throws IOException{
        String configContent;
        File file = new File(configName);
        configContent = new String(readFile(file));
        int pos, nextLine;
        String curParamValue;
        for (WRITER_CONFIG_GRAMMAR curGramma : WRITER_CONFIG_GRAMMAR.values()) {
            pos = configContent.lastIndexOf(curGramma.name());
            if (pos == -1) {
                switch (curGramma.ordinal()){
                    case 0:
                        return ERRORS.CONFIG_OUTPUT_FILE;
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
                    outputFilename = curParamValue;
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
        if (outputFilename == null){
            return ERRORS.CONFIG_STRUCTURE;
        }
        return ERRORS.NO_ERRORS;
    }
    @Override
    public Status status() {
        return status;
    }

    @Override
    public long loadDataFrom(Producer prod) {
        Producer.DataAccessor accessor = prod_access.get(prod);
        byte[] curData = (byte[])accessor.get();
        if (Arrays.equals(curData, DONE)){
            ended.incrementAndGet();
        }else {
            AtomicInteger ind = new AtomicInteger(producers.indexOf(prod));
            while (!curThread.compareAndSet(ind.get(), ind.get() + 1)){
                try{
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    logger.log("InterruptedException");
                }
            }
            inputQueue.add(curData);
        }
        return curData.length;
    }

    @Override
    public void run() {
        AtomicInteger numOfProducers = new AtomicInteger(producers.size());
        do {
            while (handled.get() < numOfProducers.get()){
                try{
                    if(inputQueue.isEmpty()){
                        Thread.sleep(100);
                    }else {
                        input = inputQueue.take();
                        if (status != Status.OK) {
                            return;
                        }
                        try {
                            write();
                            handled.incrementAndGet();
                        } catch (IOException e) {
                            logger.log(Level.SEVERE, msg("Write error"), e);
                        }
                    }
                }catch (InterruptedException e) {
                    logger.log("Interrupted error");
                }
            }
            curThread.set(0);
            handled.set(0);
        }while (ended.get() < numOfProducers.get());
    }


    @Override
    public void addProducer(Producer prod) {
        Set<String> available = prod.outputDataTypes();
        if (!available.contains(byte[].class.getCanonicalName())){
            status = Status.EXECUTOR_ERROR;
            logger.log("No byte[] output from producer " +
                    prod.getClass().getCanonicalName());
        }
        prod_access.put(prod, prod.getAccessor(byte[].class.getCanonicalName()));
        producers.add(prod);
    }

    @Override
    public void addProducers(List<Producer> producers) {
        assert producers.size() == 1;
        addProducer(producers.get(0));
    }

    private void write() throws IOException {
        if (input instanceof byte[]) {
            writeBytes();
        }
        else {
            logger.log(Level.SEVERE, msg("Unsupported data format"));
        }
    }

    private void writeBytes() throws IOException {
        assert input instanceof byte[];
        os.write((byte[]) input);
    }

    private static String msg(String msg) {
        return Writer.class.getName() + " " + msg;
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
