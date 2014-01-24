using System;

namespace EQueue.Broker
{
    public class InvalidQueueIdException : Exception
    {
        public InvalidQueueIdException(string topic, int totalAvailableQueueCount, int expectedQueueId)
            : base(string.Format("Invalid queueId of topic. Topic:{0}, Total available queue count:{1}, expected queueId:{2}", topic, totalAvailableQueueCount, expectedQueueId))
        {
        }
    }
}
