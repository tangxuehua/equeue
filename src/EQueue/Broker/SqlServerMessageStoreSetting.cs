using System;

namespace EQueue.Broker
{
    public class SqlServerMessageStoreSetting
    {
        public string ConnectionString { get; set; }
        public string MessageTable { get; set; }
        public string MessageLogFile { get; set; }
        public int PersistMessageInterval { get; set; }
        public int PersistMessageMaxCount { get; set; }
        public int RemoveExceedMaxCacheMessageFromMemoryInterval { get; set; }
        public int RemoveConsumedMessageFromMemoryInterval { get; set; }
        public int BulkCopyBatchSize { get; set; }
        public int BulkCopyTimeout { get; set; }
        public int DeleteMessageInterval { get; set; }
        public int MessageStoreMaxHours { get; set; }
        public bool IgnoreUnConsumedMessage { get; set; }
        public int BatchLoadMessageSize { get; set; }
        public int BatchLoadQueueIndexSize { get; set; }
        public long MessageMaxCacheSize { get; set; }

        public SqlServerMessageStoreSetting()
        {
            MessageTable = "Message";
            PersistMessageInterval = 1000;
            PersistMessageMaxCount = 5000;
            BulkCopyBatchSize = 5000;
            BulkCopyTimeout = 60;
            RemoveExceedMaxCacheMessageFromMemoryInterval = 1000 * 5;
            RemoveConsumedMessageFromMemoryInterval = 1000 * 5;
            DeleteMessageInterval = 1000 * 60 * 5;
            MessageStoreMaxHours = 24 * 3;
            IgnoreUnConsumedMessage = false;
            BatchLoadMessageSize = 5000;
            BatchLoadQueueIndexSize = 5000;
            MessageMaxCacheSize = 50 * 10000;
            MessageLogFile = "/home/admin/logs/equeue/message.log";
        }
    }
}
