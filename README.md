# Differences from Java implementation

We do not need builders, which simplifies classes like `RecordQueue`.

For Config, instead of having CONFIG CONFIG_DOC CONFIG_DISPLAY, we wrap these in a class.

## Batched pushing of data to sink
For columnar stores, batching data helps. So we use `flush` to control when we commit records to the sink. By comparison, the Java implementation of Kafka Connect JDBC commits on `put`. (Read [this](https://stackoverflow.com/questions/44871377/put-vs-flush-in-kafka-connector-sink-task) for difference between `put` and `flush`.)

# How sink works
We configure these upfront:
1. The database host we wish to sink to, as well as the dbuser and its password.
2. The schema we wish to sink to.
3. How often do we flush records to the database, by number of records and time since last flush--whichever condition is hit first. If these are not set, then we flush them upon receiving every collection of records from `put`.
4. Handling of backpressure (% of memory used).

With each record that comes in, we have to answer the following questions, and take the right action:
1. Which table does it belong to, and does that table have the right columns? This is determined by the record's topic. Each topic denotes a table. We check if the table exists and if its columns adhere to the schema of the record. If the table doesn't exist, we create it. If the table exists but there is a drift, e.g. missing a column, we perform an alter table command--if config allows it to do so. All these queries to the database are handled via an expression builder.
2. After finding the assigned table, we validate and add the records there.
3. If the thresholds are met, we create a prepared statement with values and commit this to the database. This should throw exceptions that prevent the task from committing offsets if the insertions fail.

We also handle backpressure from writes to the sink database. Sometimes, the sink database could struggle to ingest the data. We want Kafka Connect to pause, and stop polling.