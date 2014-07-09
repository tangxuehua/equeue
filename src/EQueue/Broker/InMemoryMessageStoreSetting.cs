namespace EQueue.Broker
{
    public class InMemoryMessageStoreSetting
    {
        public int RemoveMessageFromMemoryInterval { get; set; }

        public InMemoryMessageStoreSetting()
        {
            RemoveMessageFromMemoryInterval = 1000;
        }
    }
}
