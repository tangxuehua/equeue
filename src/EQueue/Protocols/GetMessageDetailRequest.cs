using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class GetMessageDetailRequest
    {
        public long? MessageOffset { get; private set; }
        public string MessageId { get; private set; }

        public GetMessageDetailRequest(long? messageOffset, string messageId)
        {
            MessageOffset = messageOffset;
            MessageId = messageId;
        }

        public override string ToString()
        {
            return string.Format("[MessageOffset:{0}, MessageId:{1}]", MessageOffset, MessageId);
        }
    }
}
