using System;
using EQueue.Broker;

namespace EQueue.Protocols
{
    [Serializable]
    public class TopicQueueInfo
    {
        /// <summary>主题
        /// </summary>
        public string Topic { get; set; }
        /// <summary>队列ID
        /// </summary>
        public int QueueId { get; set; }
        /// <summary>队列当前最大Offset
        /// </summary>
        public long QueueCurrentOffset { get; set; }
        /// <summary>队列当前最小Offset
        /// </summary>
        public long QueueMinOffset { get; set; }
        /// <summary>队列当前被所有消费者都消费了的最大Offset
        /// </summary>
        public long QueueMaxConsumedOffset { get; set; }
        /// <summary>队列的状态
        /// </summary>
        public QueueStatus Status { get; set; }
    }
}
