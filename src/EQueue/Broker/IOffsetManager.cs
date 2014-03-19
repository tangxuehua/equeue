namespace EQueue.Broker
{
    public interface IOffsetManager
    {
        void Start();
        void Shutdown();
        void UpdateQueueOffset(string topic, int queueId, long offset, string group);
        long GetQueueOffset(string topic, int queueId, string group);
    }
}
