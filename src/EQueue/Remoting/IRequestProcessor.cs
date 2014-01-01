namespace EQueue.Remoting
{
    public interface IRequestProcessor
    {
        RemotingResponse ProcessRequest(IRequestHandlerContext requestContext, RemotingRequest request);
    }
}
