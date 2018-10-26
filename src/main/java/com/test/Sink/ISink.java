package com.test.Sink;

import java.io.IOException;
import java.util.Map;

public interface ISink {
    void write(String table, String rowKey, Map<String,String> fields) throws IOException;
}
