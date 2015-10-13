namespace EQueue.Broker
{
    public class SqlServerOffsetManagerSetting
    {
        public string ConnectionString { get; set; }
        public string QueueOffsetTable { get; set; }
        public int PersistQueueOffsetInterval { get; set; }

        public SqlServerOffsetManagerSetting()
        {
            QueueOffsetTable = "QueueOffset";
            PersistQueueOffsetInterval = 1000;
        }
    }
}
