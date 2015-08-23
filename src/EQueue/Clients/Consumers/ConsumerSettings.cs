using System.Net;
using ECommon.Socketing;
using ECommon.Utilities;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ConsumerSetting
    {
        public IPEndPoint BrokerAddress { get; set; }
        public IPEndPoint LocalAddress { get; set; }
        public int ConsumeThreadMaxCount { get; set; }
        public int DefaultTimeoutMilliseconds { get; set; }
        public int RebalanceInterval { get; set; }
        public int UpdateTopicQueueCountInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int PersistConsumerOffsetInterval { get; set; }
        public int PullThresholdForQueue { get; set; }
        public int PullTimeDelayMillsWhenFlowControl { get; set; }
        public int SuspendPullRequestMilliseconds { get; set; }
        public int PullRequestTimeoutMilliseconds { get; set; }
        public int RetryMessageInterval { get; set; }
        public int PullMessageBatchSize { get; set; }
        public MessageHandleMode MessageHandleMode { get; set; }
        public ConsumeFromWhere ConsumeFromWhere { get; set; }

        public ConsumerSetting()
        {
            BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5001);
            ConsumeThreadMaxCount = 64;
            DefaultTimeoutMilliseconds = 60 * 1000;
            RebalanceInterval = 1000 * 5;
            HeartbeatBrokerInterval = 1000 * 5;
            UpdateTopicQueueCountInterval = 1000 * 5;
            PersistConsumerOffsetInterval = 1000 * 5;
            PullThresholdForQueue = 10000;
            PullTimeDelayMillsWhenFlowControl = 3000;
            SuspendPullRequestMilliseconds = 60 * 1000;
            PullRequestTimeoutMilliseconds = 70 * 1000;
            RetryMessageInterval = 3000;
            PullMessageBatchSize = 32;
            MessageHandleMode = MessageHandleMode.Parallel;
            ConsumeFromWhere = ConsumeFromWhere.LastOffset;
        }
    }
}
