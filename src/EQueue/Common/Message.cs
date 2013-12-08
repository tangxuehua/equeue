using System;

namespace EQueue.Common
{
    [Serializable]
    public class Message
    {
        public Guid Id { get; private set; }
        public byte[] Body { get; private set; }
        public string Topic { get; private set; }
        public long QueueOffset { get; private set; }

        public Message(Guid id, byte[] body, string topic, long queueOffset)
        {
            Id = id;
            Body = body;
            Topic = topic;
            QueueOffset = queueOffset;
        }
    }
}
