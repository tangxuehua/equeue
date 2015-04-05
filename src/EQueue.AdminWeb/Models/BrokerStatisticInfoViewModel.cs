namespace EQueue.AdminWeb.Models
{
    public class BrokerStatisticInfoViewModel
    {
        /// <summary>主题个数
        /// </summary>
        public int TopicCount { get; set; }
        /// <summary>队列个数
        /// </summary>
        public int QueueCount { get; set; }
        /// <summary>内存中的总消息数
        /// </summary>
        public long InMemoryQueueMessageCount { get; set; }
        /// <summary>未消费的总消息数
        /// </summary>
        public long UnConsumedQueueMessageCount { get; set; }
        /// <summary>消费者组个数
        /// </summary>
        public int ConsumerGroupCount { get; set; }
        /// <summary>最大全局消息偏移量
        /// </summary>
        public long CurrentMessageOffset { get; set; }
        /// <summary>已持久化的最大全局消息偏移量
        /// </summary>
        public long PersistedMessageOffset { get; set; }
        /// <summary>未持久化消息数
        /// </summary>
        public long UnPersistedMessageCount { get; set; }
        /// <summary>最小全局消息偏移量
        /// </summary>
        public long MinMessageOffset { get; set; }
    }
}