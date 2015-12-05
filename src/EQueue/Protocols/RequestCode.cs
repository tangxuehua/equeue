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
        SetProducerVisible = 1004,
        SetConsumerVisible = 1005,
        DeleteQueue = 1006,
        GetMessageDetail = 1007,
        CreateTopic = 1008,
        DeleteTopic = 1009,
        QueryProducerInfo = 1010,
        SetQueueNextConsumeOffset = 1011
    }
}
