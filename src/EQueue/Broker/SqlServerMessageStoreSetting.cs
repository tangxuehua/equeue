namespace EQueue.Broker
{
    public class SqlServerMessageStoreSetting
    {
        public string ConnectionString { get; set; }
        public string MessageTable { get; set; }
        public int CommitMessageInterval { get; set; }
        public int MessageCommitMaxCount { get; set; }
        public int DeleteMessageHourOfDay { get; set; }

        public SqlServerMessageStoreSetting()
        {
            MessageTable = "Message";
            CommitMessageInterval = 500;
            MessageCommitMaxCount = 10000;
            DeleteMessageHourOfDay = 4;
        }
    }
}
