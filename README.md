# MABigdataTest

# Requirements
maven 3.5.2+
jdk 8

# Build
mvn package

# Run
*Run with mocks*

java -cp target/consumer-1.0-jar-with-dependencies.jar com.test.PlayerEventsConsumer -m

*Run against real input/output*

java -cp target/consumer-1.0-jar-with-dependencies.jar com.test.PlayerEventsConsumer -s {KAFKA_SERVER} -h {HBASE_CONFIG_PATH} -t {TOPIC}

# Hbase
The requirement for saving the data in HBase is for it to be available for:
 - Queries on a user's events in a given timeframe
 - A daily batch job
 
 The appropriate structure for this is 2 tables:
  - user_events - key structure is "user_ID event_timestamp", each event is a row. This allows a query to define a user and a time range, and query all the relevant events by defining a startrow "userid start" and endrow "userid end", thus getting all events for this user in the timeframe.
  - daily_events - a daily table (e.g. daily_events_18_10_26), key structure is user_id, each user is a row, each event is a column. This allows a daily batch job to scan the table and get all the day's events.
  
  This means that the information is duplicated between those 2 tables, each optimized for a different scenario.
  The user_events table can be grown indefinitely, or pruned after a while (depending on size constraints.)
  The daily_events table should be created daily, and after it has been used it can be offloaded to cheap storage for backup. The events are already grouped per user so that the batch job reading it can process each user with all its information for the day. The user ID and timestamp are not written as an event field in this table because they can be fetched from the rowkey and column name, respectively.
  
  However, there are other alternative that can be easily operated, depending on circumstances and constraints:
   - If storage is an issue, or that there should be a single source of truth, the 2 tables can be merged, by having a daily version of the first table. This however requires some changes when querying, in that the query engine needs to know the all days the time frame contains, and query the appropriate tables separately.
   - If the above querying change is not feasible, then a single table can be created, but when running the batch job it should filter out the rows that represent other days' events. This is less efficient as the job needs to scan rows it doesn't need for its purpose.
   
   
  
  
