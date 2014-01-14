using System;
using EQueue.Infrastructure.Socketing;

namespace EQueue.Remoting
{
    public class SocketRequestHandlerContext : IRequestHandlerContext
    {
        public IChannel Channel { get; private set; }
        public Action<RemotingResponse> SendRemotingResponse { get; private set; }

        public SocketRequestHandlerContext(ReceiveContext receiveContext)
        {
            Channel = new SocketChannel(receiveContext.ReplySocketInfo);
            SendRemotingResponse = remotingResponse =>
            {
                receiveContext.ReplyMessage = RemotingUtil.BuildResponseMessage(remotingResponse);
                receiveContext.MessageHandledCallback(receiveContext);
            };
        }
    }
}
