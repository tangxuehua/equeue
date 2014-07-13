using ECommon.Remoting;
using ECommon.Socketing;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        public SocketSetting ProducerSocketSetting { get; set; }
        public SocketSetting ConsumerSocketSetting { get; set; }
        public SocketSetting AdminSocketSetting { get; set; }
        public bool NotifyWhenMessageArrived { get; set; }
        public int RemoveMessageInterval { get; set; }
        public int RemoveQueueIndexInterval { get; set; }
        public int SuspendPullRequestMilliseconds { get; set; }
        public int CheckBlockingPullRequestMilliseconds { get; set; }
        public int DefaultTopicQueueCount { get; set; }
        public int ScanNotActiveConsumerInterval { get; set; }
        public int ConsumerExpiredTimeout { get; set; }
        public int QueueIndexMaxCacheSize { get; set; }

        public BrokerSetting()
        {
            ProducerSocketSetting = new SocketSetting { Address = SocketUtils.GetLocalIPV4().ToString(), Port = 5000, Backlog = 5000 };
            ConsumerSocketSetting = new SocketSetting { Address = SocketUtils.GetLocalIPV4().ToString(), Port = 5001, Backlog = 5000 };
            AdminSocketSetting = new SocketSetting { Address = SocketUtils.GetLocalIPV4().ToString(), Port = 5002, Backlog = 5000 };
            NotifyWhenMessageArrived = true;
            RemoveMessageInterval = 1000 * 60 * 10;
            RemoveQueueIndexInterval = 1000 * 5;
            SuspendPullRequestMilliseconds = 1000 * 60;
            CheckBlockingPullRequestMilliseconds = 1000;
            DefaultTopicQueueCount = 4;
            ScanNotActiveConsumerInterval = 1000 * 5;
            ConsumerExpiredTimeout = 1000 * 60;
            QueueIndexMaxCacheSize = 500 * 10000;
        }
    }
}
