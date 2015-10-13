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
        /// <summary>队列当前最大位置
        /// </summary>
        public long QueueMaxOffset { get; set; }
        /// <summary>消费位置
        /// </summary>
        public long ConsumedOffset { get; set; }
        /// <summary>堆积消息数
        /// </summary>
        public long UnConsumedMessageCount { get; set; }
    }
}
