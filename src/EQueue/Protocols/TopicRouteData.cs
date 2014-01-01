using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class TopicRouteData
    {
        public int ConsumeQueueCount { get; private set; }

        public TopicRouteData(int consumeQueueCount)
        {
            ConsumeQueueCount = consumeQueueCount;
        }

        public override int GetHashCode()
        {
            var prime = 31;
            var result = 1;
            result = prime * result + ConsumeQueueCount;
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

            return true;
        }
        public override string ToString()
        {
            return string.Format("TopicRouteData[ConsumeQueueCount={0}]", ConsumeQueueCount);
        }
    }
}
