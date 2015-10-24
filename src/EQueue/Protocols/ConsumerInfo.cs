using System;
using EQueue.Broker;

namespace EQueue.Protocols
{
    [Serializable]
    public class ConsumerInfo
    {
        /// <summary>消费者的分组
        /// </summary>
        public string ConsumerGroup { get; set; }
        /// <summary>消费者的ID
        /// </summary>
        public string ConsumerId { get; set; }
        /// <summary>主题
        /// </summary>
        public string Topic { get; set; }
        /// <summary>队列ID
        /// </summary>
        public int QueueId { get; set; }
        /// <summary>队列当前位置
        /// </summary>
        public long QueueCurrentOffset { get; set; }
        /// <summary>队列消费位置
        /// </summary>
        public long ConsumedOffset { get; set; }
    }
}
