namespace EQueue.Broker
{
    public class SqlServerQueueStoreSetting
    {
        public string ConnectionString { get; set; }
        public string QueueTable { get; set; }

        public SqlServerQueueStoreSetting()
        {
            QueueTable = "Queue";
        }
    }
}
