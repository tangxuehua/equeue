using System;
using EQueue.Broker;

namespace EQueue.Protocols
{
    [Serializable]
    public class TopicConsumeInfo
    {
        /// <summary>消费者的分组
        /// </summary>
        public string ConsumerGroup { get; set; }
        /// <summary>主题
        /// </summary>
        public string Topic { get; set; }
        /// <summary>队列ID
        /// </summary>
        public int QueueId { get; set; }
        /// <summary>消费位置
        /// </summary>
        public long ConsumedOffset { get; set; }
        /// <summary>表示当前分组是否存在活跃（在线）的消费者
        /// </summary>
        public bool HasConsumer { get; set; }
    }
}
