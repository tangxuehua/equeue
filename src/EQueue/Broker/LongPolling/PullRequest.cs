using System;
using EQueue.Remoting;

namespace EQueue.Broker.LongPolling
{
    public class PullRequest
    {
        public string ConsumerGroup { get; set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }
        public IChannel ClientChannel { get; private set; }
        public DateTime SuspendTime { get; private set; }
        public long TimeoutMilliseconds { get; private set; }

        public PullRequest(string consumerGroup, string topic, int queueId, long queueOffset, IChannel clientChannel, DateTime suspendTime, long timeoutMilliseconds)
        {
            ConsumerGroup = consumerGroup;
            Topic = topic;
            QueueId = queueId;
            QueueOffset = queueOffset;
            ClientChannel = clientChannel;
            SuspendTime = suspendTime;
            TimeoutMilliseconds = timeoutMilliseconds;
        }
    }
}
