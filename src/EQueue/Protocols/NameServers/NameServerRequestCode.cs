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
        GetConsumerList = 10008
    }
}
