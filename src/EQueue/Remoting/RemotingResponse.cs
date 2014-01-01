namespace EQueue.Remoting
{
    public class RemotingResponse : RemotingMessage
    {
        public RemotingResponse(int code, byte[] body) : base(code, body) { }
    }
}
