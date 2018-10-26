package com.test.Processor;

import com.test.Sink.ISink;

public interface IProcessor {
    void Process(String record) throws ProcessException, InterruptedException;
    IProcessor toSink(ISink sink);
}
