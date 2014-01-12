using System;

namespace EQueue.Remoting
{
    public class RemotingUtil
    {
        public static byte[] BuildRequestMessage(RemotingRequest request)
        {
            var sequenceBytes = BitConverter.GetBytes(request.Sequence);
            var codeBytes = BitConverter.GetBytes(request.Code);
            var isOnewayBytes = BitConverter.GetBytes(request.IsOneway);
            var message = new byte[sequenceBytes.Length + codeBytes.Length + isOnewayBytes.Length + request.Body.Length];

            sequenceBytes.CopyTo(message, 0);
            codeBytes.CopyTo(message, sequenceBytes.Length);
            isOnewayBytes.CopyTo(message, sequenceBytes.Length + codeBytes.Length);
            request.Body.CopyTo(message, sequenceBytes.Length + codeBytes.Length + isOnewayBytes.Length);

            return message;
        }
        public static RemotingRequest ParseRequest(byte[] messageBuffer)
        {
            var sequenceBytes = new byte[8];
            var codeBytes = new byte[4];
            var isOnewayBytes = new byte[1];
            var data = new byte[messageBuffer.Length - 13];

            Array.Copy(messageBuffer, 0, sequenceBytes, 0, sequenceBytes.Length);
            Array.Copy(messageBuffer, 8, codeBytes, 0, codeBytes.Length);
            Array.Copy(messageBuffer, 12, isOnewayBytes, 0, isOnewayBytes.Length);
            Array.Copy(messageBuffer, 13, data, 0, data.Length);

            var sequence = BitConverter.ToInt64(sequenceBytes, 0);
            var code = BitConverter.ToInt32(codeBytes, 0);
            var isOneway = BitConverter.ToBoolean(isOnewayBytes, 0);

            return new RemotingRequest(code, sequence, data, isOneway);
        }

        public static byte[] BuildResponseMessage(RemotingResponse response)
        {
            var sequenceBytes = BitConverter.GetBytes(response.Sequence);
            var codeBytes = BitConverter.GetBytes(response.Code);
            var message = new byte[sequenceBytes.Length + codeBytes.Length + response.Body.Length];

            sequenceBytes.CopyTo(message, 0);
            codeBytes.CopyTo(message, sequenceBytes.Length);
            response.Body.CopyTo(message, sequenceBytes.Length + codeBytes.Length);

            return message;
        }
        public static RemotingResponse ParseResponse(byte[] messageBuffer)
        {
            var sequenceBytes = new byte[8];
            var codeBytes = new byte[4];
            var data = new byte[messageBuffer.Length - 12];

            Array.Copy(messageBuffer, 0, sequenceBytes, 0, sequenceBytes.Length);
            Array.Copy(messageBuffer, 8, codeBytes, 0, codeBytes.Length);
            Array.Copy(messageBuffer, 12, data, 0, data.Length);

            var sequence = BitConverter.ToInt64(sequenceBytes, 0);
            var code = BitConverter.ToInt32(codeBytes, 0);

            return new RemotingResponse(code, sequence, data);
        }
    }
}
