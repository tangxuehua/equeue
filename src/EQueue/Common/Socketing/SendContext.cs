using System;
using System.Net.Sockets;

namespace EQueue.Common.Socketing
{
    public class SendContext
    {
        public Socket TargetSocket { get; private set; }
        public byte[] Message { get; private set; }
        public Action<byte[]> MessageSentCallback { get; private set; }

        public SendContext(Socket targetSocket, byte[] message, Action<byte[]> messageSentCallback)
        {
            TargetSocket = targetSocket;
            Message = message;
            MessageSentCallback = messageSentCallback;
        }
    }
}
