using System;
using System.Threading.Tasks;

namespace EQueue.Remoting
{
    public interface IRemotingClient
    {
        void Start();
        void Shutdown();
        RemotingResponse InvokeSync(string address, RemotingRequest request, int timeoutMillis);
        Task<RemotingResponse> InvokeAsync(string address, RemotingRequest request, int timeoutMillis);
        void InvokeOneway(string address, RemotingRequest request, int timeoutMillis);
        void RegisterRequestProcessor(int requestCode, IRequestProcessor requestProcessor);
    }
}
