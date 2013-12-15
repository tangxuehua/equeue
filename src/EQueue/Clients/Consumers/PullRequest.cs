using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Clients.Consumers
{
    public class PullRequest
    {
        public string ConsumerGroup { get; set; }
        public MessageQueue MessageQueue { get; set; }
        public ProcessQueue ProcessQueue { get; set; }
        public long NextOffset { get; set; }

        public override int GetHashCode()
        {
            var prime = 31;
            var result = 1;
            result = prime * result + ((ConsumerGroup == null) ? 0 : ConsumerGroup.GetHashCode());
            result = prime * result + ((MessageQueue == null) ? 0 : MessageQueue.GetHashCode());
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
            PullRequest other = (PullRequest)obj;

            if (ConsumerGroup == null)
            {
                if (other.ConsumerGroup != null)
                {
                    return false;
                }
            }
            else if (!ConsumerGroup.Equals(other.ConsumerGroup))
            {
                return false;
            }

            if (MessageQueue == null)
            {
                if (other.MessageQueue != null)
                {
                    return false;
                }
            }
            else if (!MessageQueue.Equals(other.MessageQueue))
            {
                return false;
            }

            return true;
        }
        public override string ToString()
        {
            return string.Format("PullRequest [ConsumerGroup={0}, MessageQueue={1}, NextOffset={2}]", ConsumerGroup, MessageQueue, NextOffset);
        }
    }
}
