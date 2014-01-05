using System.Threading;

namespace EQueue.Remoting
{
    public class RemotingMessage
    {
        private static long _sequenceCounter;

        public int Code { get; private set; }
        public long Sequence { get; set; }
        public byte[] Body { get; private set; }

        public RemotingMessage(int code, byte[] body)
        {
            Code = code;
            Sequence = Interlocked.Increment(ref _sequenceCounter);
            Body = body;
        }
    }
}
