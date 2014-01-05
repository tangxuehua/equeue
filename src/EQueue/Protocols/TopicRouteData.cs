using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class TopicRouteData
    {
        public int QueueCount { get; private set; }

        public TopicRouteData(int queueCount)
        {
            QueueCount = queueCount;
        }

        public override int GetHashCode()
        {
            var prime = 31;
            var result = 1;
            result = prime * result + QueueCount;
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

            if (QueueCount != other.QueueCount)
            {
                return false;
            }

            return true;
        }
        public override string ToString()
        {
            return string.Format("[QueueCount={0}]", QueueCount);
        }
    }
}
