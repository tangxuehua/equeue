namespace EQueue.Protocols.Brokers
{
    public enum BrokerRequestCode
    {
        SendMessage = 10,
        PullMessage = 11,
        ProducerHeartbeat = 100,
        ConsumerHeartbeat = 101,
        GetConsumerIdsForTopic = 102,
        UpdateQueueConsumeOffsetRequest = 103,
        GetBrokerStatisticInfo = 1000,
        GetTopicQueueInfo = 1001,
        GetConsumerList = 1002,
        AddQueue = 1003,
        SetProducerVisible = 1004,
        SetConsumerVisible = 1005,
        DeleteQueue = 1006,
        GetMessageDetail = 1007,
        CreateTopic = 1008,
        DeleteTopic = 1009,
        GetProducerList = 1010,
        SetQueueNextConsumeOffset = 1011,
        DeleteConsumerGroup = 1012,
        GetTopicConsumeInfo = 1013
    }
}
