using System;
using EQueue.Broker;

namespace EQueue.Protocols
{
    public class QueueConsumedOffset
    {
        /// <summary>主题
        /// </summary>
        public string Topic { get; set; }
        /// <summary>队列ID
        /// </summary>
        public int QueueId { get; set; }
        /// <summary>队列消费进度
        /// </summary>
        public long ConsumedOffset { get; set; }
    }
}
