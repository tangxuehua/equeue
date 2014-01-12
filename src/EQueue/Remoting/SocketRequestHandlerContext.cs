using System;
using EQueue.Infrastructure;
using EQueue.Infrastructure.Socketing;

namespace EQueue.Remoting
{
    public class SocketRequestHandlerContext : IRequestHandlerContext
    {
        public Action<RemotingResponse> SendRemotingResponse { get; private set; }

        public SocketRequestHandlerContext(ReceiveContext receiveContext)
        {
            SendRemotingResponse = remotingResponse =>
            {
                receiveContext.ReplyMessage = RemotingUtil.BuildResponseMessage(remotingResponse);
                receiveContext.MessageHandledCallback(receiveContext);
            };
        }
    }
}
