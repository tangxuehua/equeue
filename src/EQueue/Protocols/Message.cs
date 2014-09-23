using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class Message
    {
        public string Topic { get; private set; }
        public int Code { get; private set; }
        public byte[] Body { get; private set; }

        public Message(string topic, int code, byte[] body)
        {
            Topic = topic;
            Code = code;
            Body = body;
        }
    }
}
