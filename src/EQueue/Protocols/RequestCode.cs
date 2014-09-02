namespace EQueue.Protocols
{
    public enum RequestCode
    {
        SendMessage = 10,
        PullMessage = 11,
        ProducerHeartbeat = 12,
        ConsumerHeartbeat = 13,
        QueryGroupConsumer = 14,
        GetTopicQueueCount = 15,
        UpdateQueueOffsetRequest = 16,
        GetTopicQueueIdsForProducer = 17,
        GetTopicQueueIdsForConsumer = 18,
        QueryTopicQueueInfo = 1001,
        QueryConsumerInfo = 1002,
        AddQueue = 1003,
        RemoveQueue = 1004,
        EnableQueue = 1005,
        DisableQueue = 1006
    }
}
