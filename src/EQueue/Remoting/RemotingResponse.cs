namespace EQueue.Remoting
{
    public class RemotingResponse
    {
        public int Code { get; private set; }
        public byte[] Body { get; private set; }

        public RemotingResponse(int code, byte[] body)
        {
            Code = code;
            Body = body;
        }
    }
}
