package com.test.Consumer;

import com.test.Processor.IProcessor;

public abstract class BaseConsumer implements IConsumer{
    protected IProcessor processor;

    protected String topicName = "";
    protected String server = "";
    protected String group = "";

    public IConsumer useProcessor(IProcessor processor) {
        this.processor = processor;
        return this;
    }

    @Override
    public IConsumer fromTopic(String topic) {
        topicName = topic;
        return this;
    }

    @Override
    public IConsumer fromServer(String server) {
        this.server = server;
        return this;
    }

    @Override
    public IConsumer withGroup(String groupID) {
        this.group = groupID;
        return this;
    }
}
