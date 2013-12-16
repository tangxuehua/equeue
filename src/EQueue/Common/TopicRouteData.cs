using System;
using System.Collections.Generic;

namespace EQueue.Common
{
    [Serializable]
    public class TopicRouteData
    {
        public int PublishQueueCount { get; private set; }
        public int ConsumeQueueCount { get; private set; }

        public TopicRouteData(int publishQueueCount, int consumeQueueCount)
        {
            PublishQueueCount = publishQueueCount;
            ConsumeQueueCount = consumeQueueCount;
        }

        public override int GetHashCode()
        {
            var prime = 31;
            var result = 1;
            result = prime * result + ConsumeQueueCount;
            result = prime * result + PublishQueueCount;
            return result;
        }
        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }

            if (GetType() != obj.GetType())
            {
                return false;
            }

            var other = (TopicRouteData)obj;

            if (ConsumeQueueCount != other.ConsumeQueueCount)
            {
                return false;
            }

            if (PublishQueueCount != other.PublishQueueCount)
            {
                return false;
            }

            return true;
        }
        public override string ToString()
        {
            return string.Format("TopicRouteData[PublishQueueCount={0}, ConsumeQueueCount={1}]", PublishQueueCount, ConsumeQueueCount);
        }
    }
}
