using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EQueue.Remoting
{
    public class SocketRemotingClient : IRemotingClient
    {
        public void Start()
        {
            throw new NotImplementedException();
        }

        public void Shutdown()
        {
            throw new NotImplementedException();
        }

        public RemotingResponse InvokeSync(string address, RemotingRequest request, long timeoutMillis)
        {
            throw new NotImplementedException();
        }

        public Task<RemotingResponse> InvokeAsync(string address, RemotingRequest request, long timeoutMillis)
        {
            throw new NotImplementedException();
        }

        public void InvokeOneway(string address, RemotingRequest request, long timeoutMillis)
        {
            throw new NotImplementedException();
        }

        public void RegisterRequestProcessor(int requestCode, IRequestProcessor requestProcessor)
        {
            throw new NotImplementedException();
        }
    }
}
