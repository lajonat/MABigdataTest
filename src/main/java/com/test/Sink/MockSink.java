package com.test.Sink;

import java.util.Map;

public class MockSink implements ISink {
    @Override
    public void write(String table, String rowKey, Map<String, String> fields) {
        System.out.println(String.format("Writing to table %s under %s", table, rowKey));
        for ( Map.Entry<String, String> f : fields.entrySet()){
            System.out.println(String.format("\t%s : %s", f.getKey(), f.getValue()));
        }
    }
}
