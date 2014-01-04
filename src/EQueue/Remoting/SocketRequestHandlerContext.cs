using System;
using EQueue.Infrastructure;
using EQueue.Infrastructure.Socketing;

namespace EQueue.Remoting
{
    public class SocketRequestHandlerContext : IRequestHandlerContext
    {
        public Action<RemotingResponse> SendRemotingResponse { get; private set; }

        public SocketRequestHandlerContext(IBinarySerializer binarySerializer, ReceiveContext receiveContext)
        {
            SendRemotingResponse = remotingResponse =>
            {
                receiveContext.ReplyMessage = binarySerializer.Serialize(remotingResponse);
                receiveContext.MessageHandledCallback(receiveContext);
            };
        }
    }
}
