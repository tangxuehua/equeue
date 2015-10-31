using System;
using EQueue.Utils;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        public long LogPosition { get; set; }
        public string MessageId { get; set; }
        public int QueueId { get; set; }
        public long QueueOffset { get; set; }
        public DateTime StoredTime { get; set; }

        public QueueMessage() { }
        public QueueMessage(string messageId, string topic, int code, byte[] body, int queueId, long queueOffset, DateTime createdTime, DateTime storedTime, string tag)
            : base(topic, code, body, createdTime, tag)
        {
            MessageId = messageId;
            QueueId = queueId;
            QueueOffset = queueOffset;
            StoredTime = storedTime;
        }

        public virtual void ReadFrom(byte[] recordBuffer)
        {
            var srcOffset = 0;

            LogPosition = MessageUtils.DecodeLong(recordBuffer, srcOffset, out srcOffset);
            MessageId = MessageUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Topic = MessageUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Tag = MessageUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Code = MessageUtils.DecodeInt(recordBuffer, srcOffset, out srcOffset);
            Body = MessageUtils.DecodeBytes(recordBuffer, srcOffset, out srcOffset);
            QueueId = MessageUtils.DecodeInt(recordBuffer, srcOffset, out srcOffset);
            QueueOffset = MessageUtils.DecodeLong(recordBuffer, srcOffset, out srcOffset);
            CreatedTime = MessageUtils.DecodeDateTime(recordBuffer, srcOffset, out srcOffset);
            StoredTime = MessageUtils.DecodeDateTime(recordBuffer, srcOffset, out srcOffset);
        }
        public bool IsValid()
        {
            return !string.IsNullOrEmpty(MessageId);
        }

        public override string ToString()
        {
            return string.Format("[Topic={0},QueueId={1},QueueOffset={2},MessageId={3},LogPosition={4},Code={5},CreatedTime={6},StoredTime={7},BodyLength={8},Tag={9}]",
                Topic,
                QueueId,
                QueueOffset,
                MessageId,
                LogPosition,
                Code,
                CreatedTime,
                StoredTime,
                Body.Length,
                Tag);
        }
    }
}
