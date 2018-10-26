package com.test.Consumer;

import com.test.Processor.ProcessException;

/**
 * Mocks reading of records from kafka
 */
public class MockConsumer extends BaseConsumer  {

    private String[] mockRecords = {
      "{\"user_id\":\"abc\",\"event\":\"Login\",\"time\":1234,\"ip\":\"1.2.3.4\",\"params\":\"test\"}",
            "{\"user_id\":\"abc\",\"event\":\"Attack\",\"time\":1235,\"ip\":\"1.2.3.4\",\"params\":\"test2\"}",
            "{\"user_id\":\"dssa\",\"event\":\"Login\",\"time\":1236,\"ip\":\"1.2.3.1\",\"params\":\"test3\"}",
            "{\"user_id\":\"abc\",\"event\":\"Defend\",\"time\":1237,\"ip\":\"1.2.3.4\",\"params\":\"test4\"}",
            "{\"user_id\":\"dssa\",\"event\":\"Logout\",\"time\":1238,\"ip\":\"1.2.3.1\",\"params\":\"test5\"}",
            "{\"user_id\":\"feb\",\"event\":\"Buy\",\"time\":1239,\"ip\":\"2.2.3.4\",\"params\":\"test6\"}"
    };
    public void Start() {
        try {
            for (String rec : mockRecords) {
                processor.Process(rec);
            }
        } catch (ProcessException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
