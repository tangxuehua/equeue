namespace EQueue.Remoting
{
    public class RemotingResponse : RemotingMessage
    {
        public RemotingResponse(int code, long sequence, byte[] body) : base(code, sequence, body) { }

        public override string ToString()
        {
            return string.Format("[Code:{0}, Sequence:{1}]", Code, Sequence);
        }
    }
}
