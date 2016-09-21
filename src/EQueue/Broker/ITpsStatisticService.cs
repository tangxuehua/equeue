namespace EQueue.Broker
{
    public interface ITpsStatisticService
    {
        void AddTopicSendCount(string topic, int queueId);
        void UpdateTopicConsumeOffset(string topic, int queueId, string consumeGroup, long consumeOffset);
        long GetTopicSendThroughput(string topic, int queueId);
        long GetTopicConsumeThroughput(string topic, int queueId, string consumeGroup);
        long GetTotalSendThroughput();
        long GetTotalConsumeThroughput();
        void Start();
        void Shutdown();
    }
}
