namespace EQueue.Remoting
{
    public class RemotingRequest : RemotingMessage
    {
        public bool IsOneway { get; set; }

        public RemotingRequest(int code, byte[] body) : base(code, body) { }

        public override string ToString()
        {
            return string.Format("[Code:{0}, Sequence:{1}]", Code, Sequence);
        }
    }
}
