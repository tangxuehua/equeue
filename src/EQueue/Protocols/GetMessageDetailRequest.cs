using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class GetMessageDetailRequest
    {
        public long MessageOffset { get; private set; }

        public GetMessageDetailRequest(long messageOffset)
        {
            MessageOffset = messageOffset;
        }

        public override string ToString()
        {
            return string.Format("[MessageOffset:{0}]", MessageOffset);
        }
    }
}
