using System;

namespace EQueue.Utils
{
    public class QueueKeyUtil
    {
        public static string CreateQueueKey(string topic, int queueId)
        {
            return string.Format("{0}-{1}", topic, queueId);
        }
        public static string[] ParseQueueKey(string queueKey)
        {
            return queueKey.Split(new string[] { "-" }, StringSplitOptions.None);
        }
    }
}
