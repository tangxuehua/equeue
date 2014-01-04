using System;

namespace EQueue.Remoting
{
    public interface IRequestHandlerContext
    {
        Action<RemotingResponse> SendRemotingResponse { get; }
    }
}
