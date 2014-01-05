namespace EQueue.Remoting
{
    public interface IRequestProcessor
    {
        RemotingResponse ProcessRequest(IRequestHandlerContext context, RemotingRequest request);
    }
}
