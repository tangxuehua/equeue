using System;
using System.Net.Sockets;

namespace EQueue.Common.Socketing
{
    public class ReceiveContext
    {
        public Socket ReplySocket { get; private set; }
        public byte[] ReceivedMessage { get; private set; }
        public byte[] ReplyMessage { get; set; }
        public Action<ReceiveContext> MessageHandledCallback { get; private set; }

        public ReceiveContext(Socket replySocket, byte[] receivedMessage, Action<ReceiveContext> messageHandledCallback)
        {
            ReplySocket = replySocket;
            ReceivedMessage = receivedMessage;
            MessageHandledCallback = messageHandledCallback;
        }
    }
}
