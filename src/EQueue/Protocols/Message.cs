using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class Message
    {
        public string Topic { get; private set; }
        public byte[] Body { get; private set; }

        public Message(string topic, byte[] body)
        {
            Topic = topic;
            Body = body;
        }
    }
}
