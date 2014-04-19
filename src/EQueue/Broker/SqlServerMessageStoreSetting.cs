namespace EQueue.Broker
{
    public class SqlServerMessageStoreSetting
    {
        public string ConnectionString { get; set; }
        public string MessageTable { get; set; }
        public int CommitMessageInterval { get; set; }
        public int MessageCommitMaxCount { get; set; }
        public int DeleteMessageInterval { get; set; }
        public int DeleteMessageHourOfDay { get; set; }

        public SqlServerMessageStoreSetting()
        {
            CommitMessageInterval = 500;
            MessageCommitMaxCount = 10000;
            DeleteMessageInterval = 5 * 60 * 1000;
            DeleteMessageHourOfDay = 4;
        }
    }
}
