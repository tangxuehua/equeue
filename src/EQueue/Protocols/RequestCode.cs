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
        QueryTopicQueueInfo = 1001,
        QueryTopicConsumeInfo = 1002
    }
}
