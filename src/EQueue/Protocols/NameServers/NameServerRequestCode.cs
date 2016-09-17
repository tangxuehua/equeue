namespace EQueue.Protocols.NameServers
{
    public enum NameServerRequestCode
    {
        RegisterBroker = 10000,
        UnregisterBroker = 10001,
        GetAllClusters = 10002,
        GetClusterBrokers = 10003,
        GetTopicRouteInfo = 10004,
        GetTopicQueueInfo = 10005,
        GetTopicConsumeInfo = 10006,
        GetProducerList = 10007,
        GetConsumerList = 10008,
        CreateTopic = 10009,
        DeleteTopic = 10010,
        AddQueue = 10011,
        DeleteQueue = 10012,
        SetQueueProducerVisible = 10013,
        SetQueueConsumerVisible = 10014,
        SetQueueNextConsumeOffset = 10015,
        DeleteConsumerGroup = 10016
    }
}
