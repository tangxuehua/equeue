namespace EQueue.Remoting
{
    public enum RequestCode
    {
        SendMessage = 10,
        PullMessage = 11,
        ProducerHeartbeat = 12,
        ConsumerHeartbeat = 13,
        QueryGroupConsumer = 14,
        GetTopicQueueCount = 15,
    }
}
