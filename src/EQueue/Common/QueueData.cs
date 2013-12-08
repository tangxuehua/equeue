using System;
using System.Collections.Generic;

namespace EQueue.Common
{
    public class QueueData
    {
        public int PublishQueueCount { get; private set; }
        public int ConsumeQueueCount { get; private set; }

        public QueueData(int publishQueueCount, int consumeQueueCount)
        {
            PublishQueueCount = publishQueueCount;
            ConsumeQueueCount = consumeQueueCount;
        }
    }
}
