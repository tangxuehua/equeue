namespace EQueue.Remoting
{
    public class RemotingMessage
    {
        public long Sequence { get; private set; }
        public int Code { get; private set; }
        public byte[] Body { get; private set; }

        public RemotingMessage(int code, long sequence, byte[] body)
        {
            Code = code;
            Sequence = sequence;
            Body = body;
        }
    }
}
