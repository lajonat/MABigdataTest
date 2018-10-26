package com.test;

import com.test.Consumer.IConsumer;
import com.test.Consumer.KafkaTopicConsumer;
import com.test.Consumer.MockConsumer;
import com.test.Processor.PlayerEventsProcessor;
import com.test.Sink.HBaseSink;
import com.test.Sink.ISink;
import com.test.Sink.MockSink;
import org.apache.commons.cli.*;

/**
 * This consumer reads player events from Kafka topics, and writes them formatted to the appropriate hbase tables
 */
public class PlayerEventsConsumer
{
    // Arguments
    private static boolean mock = false;
    private static String topic = "player_events";
    private static String server = "localhost:9092";
    private static String group = "player_events_group";
    private static String hbaseConfig = "hbase-site.xml";

    public static void main( String[] args )
    {
        parseArgs(args);

        IConsumer consumer;
        ISink target;
        if (mock){
            consumer = new MockConsumer();
            target = new MockSink();
        } else{
            consumer = new KafkaTopicConsumer();
            target = new HBaseSink(hbaseConfig);
        }

        PlayerEventsProcessor processor = new PlayerEventsProcessor();
        processor.toSink(target);

        consumer
                .fromServer(server)
                .fromTopic(topic)
                .withGroup(group)
                .useProcessor(processor)
        .Start();

        System.out.println("Finished");
    }

    private static void parseArgs(String[] args) {
        Options options = new Options();

        Option mockOpt = new Option("m", "mock", false, "Whether to mock input/output");
        options.addOption(mockOpt);

        Option topicOpt = new Option("t", "topic", true, "Topic to read data from");
        options.addOption(topicOpt);

        Option serverOpt = new Option("s", "server", true, "Kafka broker server");
        options.addOption(serverOpt);

        Option groupOpt = new Option("g", "group", true, "Group ID");
        options.addOption(groupOpt);

        Option hbaseOpt = new Option("h", "hbase", true, "Path of hbase-site.xml config file");
        options.addOption(hbaseOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        mock = cmd.hasOption("mock");
        if (cmd.hasOption("topic")){
            topic = cmd.getOptionValue("topic");
        }
        if (cmd.hasOption("server")){
            server = cmd.getOptionValue("server");
        }
        if (cmd.hasOption("group")){
            group = cmd.getOptionValue("group");
        }
        if (cmd.hasOption("hbase")){
            hbaseConfig = cmd.getOptionValue("hbase");
        }
    }
}
