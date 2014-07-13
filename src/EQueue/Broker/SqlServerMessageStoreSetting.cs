namespace EQueue.Broker
{
    public class SqlServerMessageStoreSetting
    {
        public string ConnectionString { get; set; }
        public string MessageTable { get; set; }
        public int PersistMessageInterval { get; set; }
        public int PersistMessageMaxCount { get; set; }
        public int RemoveMessagesFromMemoryInterval { get; set; }
        public int RemoveConsumedMessagesFromMemoryInterval { get; set; }
        public int DeleteMessageInterval { get; set; }
        public int BulkCopyBatchSize { get; set; }
        public int BulkCopyTimeout { get; set; }
        public int DeleteMessageHourOfDay { get; set; }
        public int BatchLoadMessageSize { get; set; }
        public int BatchLoadQueueIndexSize { get; set; }
        public long MessageMaxCacheSize { get; set; }

        public SqlServerMessageStoreSetting()
        {
            MessageTable = "Message";
            PersistMessageInterval = 500;
            PersistMessageMaxCount = 10000;
            BulkCopyBatchSize = 10000;
            BulkCopyTimeout = 10;
            RemoveMessagesFromMemoryInterval = 5000;
            RemoveConsumedMessagesFromMemoryInterval = 5000;
            DeleteMessageInterval = 1000 * 60 * 10;
            DeleteMessageHourOfDay = 4;
            BatchLoadMessageSize = 5000;
            BatchLoadQueueIndexSize = 5000;
            MessageMaxCacheSize = 100 * 10000;
        }
    }
}
