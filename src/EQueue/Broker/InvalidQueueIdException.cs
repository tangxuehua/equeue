using System;
using System.Collections.Generic;

namespace EQueue.Broker
{
    public class InvalidQueueIdException : Exception
    {
        public InvalidQueueIdException(string topic, IEnumerable<int> availableQueueIds, int expectedQueueId)
            : base(string.Format("Invalid queueId of topic. Topic:{0}, Total available queueIds:{1}, expected queueId:{2}", topic, string.Join(":", availableQueueIds), expectedQueueId))
        {
        }
    }
}
