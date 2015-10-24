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
        /// <summary>队列当前被所有消费者都消费了的最小Offset
        /// </summary>
        public long QueueMinConsumedOffset { get; set; }
        /// <summary>对生产者是否可见
        /// </summary>
        public bool ProducerVisible { get; set; }
        /// <summary>对消费者是否可见
        /// </summary>
        public bool ConsumerVisible { get; set; }
    }
}
