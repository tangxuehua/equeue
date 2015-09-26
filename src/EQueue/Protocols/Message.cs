using System;
using ECommon.Utilities;

namespace EQueue.Protocols
{
    [Serializable]
    public class Message
    {
        public string Topic { get; set; }
        public int Code { get; set; }
        public string Key { get; set; }
        public byte[] Body { get; set; }
        public DateTime CreatedTime { get; set; }

        public Message() { }
        public Message(string topic, int code, string key, byte[] body) : this(topic, code, key, body, DateTime.Now) { }
        public Message(string topic, int code, string key, byte[] body, DateTime createdTime)
        {
            Ensure.NotNull(topic, "topic");
            Ensure.Positive(code, "code");
            Ensure.NotNull(key, "key");
            Ensure.NotNull(body, "body");
            Topic = topic;
            Code = code;
            Key = key;
            Body = body;
            CreatedTime = createdTime;
        }

        public override string ToString()
        {
            return string.Format("[Topic={0},Code={1},Key={2},CreatedTime={3},BodyLength={4}]", Topic, Code, Key, CreatedTime, Body.Length);
        }
    }
}
