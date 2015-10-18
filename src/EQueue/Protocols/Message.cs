using System;
using ECommon.Utilities;

namespace EQueue.Protocols
{
    [Serializable]
    public class Message
    {
        public string Topic { get; set; }
        public int Code { get; set; }
        public byte[] Body { get; set; }
        public DateTime CreatedTime { get; set; }

        public Message() { }
        public Message(string topic, int code, byte[] body) : this(topic, code, body, DateTime.Now) { }
        public Message(string topic, int code, byte[] body, DateTime createdTime)
        {
            Ensure.NotNull(topic, "topic");
            Ensure.Positive(code, "code");
            Ensure.NotNull(body, "body");
            Topic = topic;
            Code = code;
            Body = body;
            CreatedTime = createdTime;
        }

        public override string ToString()
        {
            return string.Format("[Topic={0},Code={1},CreatedTime={2},BodyLength={3}]", Topic, Code, CreatedTime, Body.Length);
        }
    }
}
