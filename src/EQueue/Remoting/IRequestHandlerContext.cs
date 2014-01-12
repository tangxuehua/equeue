using System;

namespace EQueue.Remoting
{
    public interface IRequestHandlerContext
    {
        IChannel Channel { get; }
        Action<RemotingResponse> SendRemotingResponse { get; }
    }
}
