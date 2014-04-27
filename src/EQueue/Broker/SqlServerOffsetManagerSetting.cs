namespace EQueue.Broker
{
    public class SqlServerOffsetManagerSetting
    {
        public string ConnectionString { get; set; }
        public string QueueOffsetTable { get; set; }
        public int CommitQueueOffsetInterval { get; set; }

        public SqlServerOffsetManagerSetting()
        {
            QueueOffsetTable = "QueueOffset";
            CommitQueueOffsetInterval = 5000;
        }
    }
}
