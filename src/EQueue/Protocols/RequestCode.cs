namespace EQueue.Protocols
{
    public enum RequestCode
    {
        SendMessage = 10,
        PullMessage = 11,
        ProducerHeartbeat = 100,
        ConsumerHeartbeat = 101,
        QueryGroupConsumer = 102,
        GetTopicQueueCount = 103,
        UpdateQueueOffsetRequest = 104,
        GetTopicQueueIdsForProducer = 105,
        GetTopicQueueIdsForConsumer = 106,
        QueryBrokerStatisticInfo = 1000,
        QueryTopicQueueInfo = 1001,
        QueryConsumerInfo = 1002,
        AddQueue = 1003,
        RemoveQueue = 1004,
        EnableQueue = 1005,
        DisableQueue = 1006,
        QueryTopicConsumeInfo = 1007,
        QueryMessage = 1008,
        GetMessageDetail = 109,
        CreateTopic = 1010,
    }
}
