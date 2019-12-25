package ru.spbstu.pipeline;

import java.util.Map;

/**
 * Reads data in loop.
 */
public interface Reader extends Producer, InitializableProducer, Runnable {
    public void run();
}
