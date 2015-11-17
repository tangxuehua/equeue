using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class BrokerStatisticInfo
    {
        /// <summary>主题个数
        /// </summary>
        public int TopicCount { get; set; }
        /// <summary>队列个数
        /// </summary>
        public int QueueCount { get; set; }
        /// <summary>生产者个数
        /// </summary>
        public int ProducerCount { get; set; }
        /// <summary>消费者组个数
        /// </summary>
        public int ConsumerGroupCount { get; set; }
        /// <summary>消费者个数
        /// </summary>
        public int ConsumerCount { get; set; }
        /// <summary>未消费消息总数
        /// </summary>
        public long TotalUnConsumedMessageCount { get; set; }
        /// <summary>消息Chunk文件总数
        /// </summary>
        public int MessageChunkCount { get; set; }
        /// <summary>消息最小Chunk
        /// </summary>
        public int MessageMinChunkNum { get; set; }
        /// <summary>消息最大Chunk
        /// </summary>
        public int MessageMaxChunkNum { get; set; }
    }
}
