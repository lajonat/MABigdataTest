package com.test.Sink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * General purpose HBase sink - writes single rows to the given table
 * A few improvements can be made if needed:
 *      - Batching of several rows to one put
 *      - Threading support (currently naive, single thread)
 *      - More validation checks
 *      - Table timeouts, reconnections
 */
public class HBaseSink implements ISink {
    private Connection hbaseConnection;
    private Map<String, Table> tableMap = new HashMap<>();

    public HBaseSink(String configPath) {
        Configuration config = HBaseConfiguration.create();
        config.addResource(configPath);

        try {
            HBaseAdmin.available(config);
            hbaseConnection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            throw new RuntimeException("Could not connect to HBase using config in " + configPath);
        }

        // When shutting the process down, release all connections
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                System.out.println("Shutting down HBase connections");
                for (Table table : tableMap.values()){
                    try {
                        table.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    hbaseConnection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private Table getTable(String tableName) throws IOException {
        if (!tableMap.containsKey(tableName)){
            Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
            tableMap.put(tableName, table);
        }

        return tableMap.get(tableName);
    }

    /**
     * Writes a records to the given hbase table
     * @param tableName name of the table to write to
     * @param rowKey rowkey to write the values under
     * @param fields - a map of column -> value to write, where column is family:column
     * @throws IOException - on any problem saving to hbase
     */
    @Override
    public void write(String tableName, String rowKey, Map<String, String> fields) throws IOException {
        Table table =  getTable(tableName);
        Put p = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, String> f : fields.entrySet()){
            // TODO: Handle invalid column definitions (throw exception, use default)
            String[] columnQualifiers = f.getKey().split(":");
            p.addColumn(
                    Bytes.toBytes(columnQualifiers[0]),
                    Bytes.toBytes(columnQualifiers[1]),
                    Bytes.toBytes(f.getValue()));
        }
        table.put(p);
    }
}
