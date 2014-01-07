using System;
using System.Net.Sockets;

namespace EQueue.Infrastructure.Socketing
{
    public class ReceiveContext
    {
        public SocketInfo ReplySocketInfo { get; private set; }
        public byte[] ReceivedMessage { get; private set; }
        public byte[] ReplyMessage { get; set; }
        public Action<ReceiveContext> MessageHandledCallback { get; private set; }

        public ReceiveContext(SocketInfo replySocketInfo, byte[] receivedMessage, Action<ReceiveContext> messageHandledCallback)
        {
            ReplySocketInfo = replySocketInfo;
            ReceivedMessage = receivedMessage;
            MessageHandledCallback = messageHandledCallback;
        }
    }
}
