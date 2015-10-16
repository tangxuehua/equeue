using System;
using System.Collections.Generic;

namespace EQueue.Broker.Exceptions
{
    public class QueueNotExistException : Exception
    {
        public QueueNotExistException(string topic, int queueId)
            : base(string.Format("Queue not exist. topic:{0}, queueId:{1}", topic, queueId))
        {
        }
    }
}
