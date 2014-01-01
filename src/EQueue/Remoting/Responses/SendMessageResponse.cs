namespace EQueue.Remoting.Responses
{
    public class SendMessageResponse
    {
        public string MessageId { get; private set; }
        public long MessageOffset { get; private set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public SendMessageResponse(string messageId, long messageOffset, string topic, int queueId, long queueOffset)
        {
            MessageId = messageId;
            MessageOffset = messageOffset;
            Topic = topic;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }
    }
}
