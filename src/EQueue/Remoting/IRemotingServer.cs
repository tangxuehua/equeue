using System.Threading.Tasks;

namespace EQueue.Remoting
{
    public interface IRemotingServer
    {
        void Start();
        void Shutdown();
        RemotingResponse InvokeSync(Channel channel, RemotingRequest request, long timeoutMillis);
        Task<RemotingResponse> InvokeAsync(Channel channel, RemotingRequest request, long timeoutMillis);
        void InvokeOneway(Channel channel, RemotingRequest request, long timeoutMillis);
        void RegisterRequestProcessor(int requestCode, IRequestProcessor requestProcessor);
    }
}
