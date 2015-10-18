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
        /// <summary>消费者组个数
        /// </summary>
        public int ConsumerGroupCount { get; set; }
        /// <summary>消费者个数
        /// </summary>
        public int ConsumerCount { get; set; }
        /// <summary>消息最大位置
        /// </summary>
        public long CurrentMessagePosition { get; set; }
        /// <summary>消息最小位置
        /// </summary>
        public long MinMessageOffset { get; set; }
        /// <summary>消息消费滑动门起始位置
        /// </summary>
        public long MinConsumedMessagePosition { get; set; }
        /// <summary>消息Chunk文件总数
        /// </summary>
        public int MessageChunkCount { get; set; }
        /// <summary>消息最小Chunk号
        /// </summary>
        public int MessageMinChunkNum { get; set; }
        /// <summary>消息最大Chunk号
        /// </summary>
        public int MessageMaxChunkNum { get; set; }
    }
}
