package com.test.Processor;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.test.Message.PlayerEventMessage;
import com.test.Sink.ISink;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Processes player events, reads them and writes the appropriate data to hbase tables
 * one table for daily batch jobs
 * one table for user timeframe queries
 * Improvements to be made:
 *      - Multithreading support (specifically SimpleDateFormat is not threadsafe)
 *      - Handle delayed messages differently? (if we do not want to save late arrivals)
 *      - Better handling of errors against hbase - not an infinite loop? Differentiate between recoverable and unrecoverable errors
 *      - Better definition of daily events - if 2 events happen at the same exact timestamp for the same users, they will override each other
 */
public class PlayerEventsProcessor implements IProcessor {
    private ISink target;
    private Gson gson = new Gson();
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yy_MM_dd");

    @Override
    public IProcessor toSink(ISink sink) {
        target = sink;
        return this;
    }

    @Override
    public void Process(String record) throws ProcessException, InterruptedException {
        PlayerEventMessage message;
        try {
            message = gson.fromJson(record, PlayerEventMessage.class);
        } catch (JsonSyntaxException e){
            throw new ProcessException("Could not parse record", e);
        }

        if (!message.isValid()){
            throw new ProcessException("Invalid record");
        }

        // TODO: Handle delayed messages? Should we write messages that are more than 1 day late?

        Map<String, String> outputData = getOutput(message);
        Map<String, String> dailyOutputData = getDailyOutput(message);

        // If an exception happens while writing to hbase, assume it is temporary, and wait for it to be resolved.
        // Do not continue to next record until then, so that we won't lose records (by reading them and not writing)
        while (true) {
            try {
                target.write("user_events", message.getUser_id() + " " + message.getTime(), outputData);
                target.write("daily_events_" + dateFormatter.format(new Date()), message.getUser_id(), dailyOutputData);
                break;
            } catch (IOException e) {
                // LOG AND ALERT HERE
                e.printStackTrace();
                // TODO: exponential backoff?
                Thread.sleep(5000);
            }
        }
    }

    private Map<String, String> getDailyOutput(PlayerEventMessage message) {
        Map<String, String> output = new HashMap<>();
        // TODO: Better definition of daily events
        String value = String.format("{\"event\":\"%s\",\"ip\":\"%s\",\"params\":\"%s\"}",
                message.getEvent(), message.getIp(), message.getParams());
        output.put("data:" + message.getTime(), value);
        return output;
    }

    private Map<String, String> getOutput(PlayerEventMessage message) {
        Map<String, String> output = new HashMap<>();
        output.put("data:event", message.getEvent());
        output.put("data:time", message.getTime());
        output.put("data:ip", message.getIp());
        output.put("data:params", message.getParams());
        return output;
    }
}
