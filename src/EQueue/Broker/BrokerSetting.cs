using System.Net;
using ECommon.Utilities;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        public IPEndPoint ProducerIPEndPoint { get; set; }
        public IPEndPoint ConsumerIPEndPoint { get; set; }
        public IPEndPoint AdminIPEndPoint { get; set; }
        public bool NotifyWhenMessageArrived { get; set; }
        public int RemoveConsumedMessageInterval { get; set; }
        public int RemoveExceedMaxCacheQueueIndexInterval { get; set; }
        public int CheckBlockingPullRequestMilliseconds { get; set; }
        public int NotifyMessageArrivedThreadMaxCount { get; set; }
        public int ScanNotActiveConsumerInterval { get; set; }
        public int ConsumerExpiredTimeout { get; set; }
        public int QueueIndexMaxCacheSize { get; set; }
        public int TopicMaxQueueCount { get; set; }

        public BrokerSetting()
        {
            ProducerIPEndPoint = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000);
            ConsumerIPEndPoint = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5001);
            AdminIPEndPoint = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);
            NotifyWhenMessageArrived = true;
            RemoveConsumedMessageInterval = 1000 * 5;
            RemoveExceedMaxCacheQueueIndexInterval = 1000 * 5;
            CheckBlockingPullRequestMilliseconds = 1000;
            NotifyMessageArrivedThreadMaxCount = 32;
            ScanNotActiveConsumerInterval = 1000 * 5;
            ConsumerExpiredTimeout = 1000 * 60;
            QueueIndexMaxCacheSize = 500 * 10000;
            TopicMaxQueueCount = 1024;
        }
    }
}
