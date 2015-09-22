using System.Net;
using ECommon.Socketing;
using EQueue.Broker.Storage;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        public IPEndPoint ProducerAddress { get; set; }
        public IPEndPoint ConsumerAddress { get; set; }
        public IPEndPoint AdminAddress { get; set; }
        public bool NotifyWhenMessageArrived { get; set; }
        public int DeleteQueueMessagesInterval { get; set; }
        public int DeleteMessagesInterval { get; set; }
        public int CheckBlockingPullRequestMilliseconds { get; set; }
        public int NotifyMessageArrivedThreadMaxCount { get; set; }
        public int ScanNotActiveConsumerInterval { get; set; }
        public int ConsumerExpiredTimeout { get; set; }
        public int QueueIndexMaxCacheSize { get; set; }
        public bool AutoCreateTopic { get; set; }
        public int TopicDefaultQueueCount { get; set; }
        public int TopicMaxQueueCount { get; set; }
        public TFChunkManagerConfig MessageChunkConfig { get; set; }
        public TFChunkManagerConfig QueueChunkConfig { get; set; }

        public BrokerSetting()
        {
            ProducerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000);
            ConsumerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5001);
            AdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);

            NotifyWhenMessageArrived = true;
            DeleteQueueMessagesInterval = 1000 * 5;
            DeleteMessagesInterval = 1000 * 5;
            CheckBlockingPullRequestMilliseconds = 1000;
            NotifyMessageArrivedThreadMaxCount = 32;
            ScanNotActiveConsumerInterval = 1000 * 5;
            ConsumerExpiredTimeout = 1000 * 60;
            QueueIndexMaxCacheSize = 20 * 10000;
            AutoCreateTopic = true;
            TopicDefaultQueueCount = 4;
            TopicMaxQueueCount = 64;

            MessageChunkConfig = TFChunkManagerConfig.Create(@"d:\equeue-store\message-chunks", "message-chunk-", 256 * 1024 * 1024, 0, 0);
            QueueChunkConfig = TFChunkManagerConfig.Create(@"d:\equeue-store\queue-chunks", "queue-chunk-", 0, 8, 1000000);
        }
    }
}
