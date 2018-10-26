package com.test.Consumer;

import com.test.Processor.IProcessor;

public interface IConsumer {
    void Start();
    IConsumer useProcessor(IProcessor processor);
    IConsumer fromTopic(String topic);
    IConsumer fromServer(String server);
    IConsumer withGroup(String groupID);
}
