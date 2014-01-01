using System;
using System.Net.Sockets;

namespace EQueue.Infrastructure.Socketing
{
    public class SendContext
    {
        public Socket TargetSocket { get; private set; }
        public byte[] Message { get; private set; }
        public Action<SendResult> MessageSendCallback { get; private set; }

        public SendContext(Socket targetSocket, byte[] message, Action<SendResult> messageSendCallback)
        {
            TargetSocket = targetSocket;
            Message = message;
            MessageSendCallback = messageSendCallback;
        }
    }
}
