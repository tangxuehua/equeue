namespace EQueue.Broker
{
    public class InMemoryMessageStoreSetting
    {
        public int RemoveMessageFromMemoryInterval { get; set; }
        public long MessageMaxCacheSize { get; set; }

        public InMemoryMessageStoreSetting()
        {
            RemoveMessageFromMemoryInterval = 1000 * 5;
            MessageMaxCacheSize = 20 * 10000;
        }
    }
}
