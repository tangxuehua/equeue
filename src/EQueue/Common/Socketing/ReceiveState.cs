using System;
using System.Collections.Generic;
using System.Net.Sockets;

namespace EQueue.Common.Socketing
{
    public class ReceiveState
    {
        public const int BufferSize = 1024;
        public byte[] Buffer = new byte[BufferSize];
        public List<byte> Data = new List<byte>();
        public int? MessageSize;
        public Socket SourceSocket { get; private set; }
        public Action<byte[]> MessageReceivedCallback { get; private set; }

        public ReceiveState(Socket sourceSocket, Action<byte[]> messageReceivedCallback)
        {
            SourceSocket = sourceSocket;
            MessageReceivedCallback = messageReceivedCallback;
        }
    }
}
