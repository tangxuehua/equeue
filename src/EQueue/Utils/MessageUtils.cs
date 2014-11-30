using System;
using System.Linq;
using System.Text;
using EQueue.Protocols;

namespace EQueue.Utils
{
    public class MessageUtils
    {
        public static byte[] EncodeSendMessageRequest(SendMessageRequest request)
        {
            var queueIdBytes = BitConverter.GetBytes(request.QueueId);
            var codeBytes = BitConverter.GetBytes(request.Message.Code);
            var topicBytes = Encoding.UTF8.GetBytes(request.Message.Topic);
            var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);
            var routingKeyBytes = Encoding.UTF8.GetBytes(request.RoutingKey);
            var routingKeyLengthBytes = BitConverter.GetBytes(routingKeyBytes.Length);
            return Combine(queueIdBytes, codeBytes, topicLengthBytes, topicBytes, routingKeyLengthBytes, routingKeyBytes, request.Message.Body);
        }
        public static SendMessageRequest DecodeSendMessageRequest(byte[] messageBuffer)
        {
            var queueIdBytes = new byte[4];
            var codeBytes = new byte[4];
            var topicLengthBytes = new byte[4];
            var routingKeyLengthBytes = new byte[4];
            var headerLength = 0;

            Buffer.BlockCopy(messageBuffer, 0, queueIdBytes, 0, 4);
            headerLength += 4;

            Buffer.BlockCopy(messageBuffer, headerLength, codeBytes, 0, 4);
            headerLength += 4;

            Buffer.BlockCopy(messageBuffer, headerLength, topicLengthBytes, 0, 4);
            headerLength += 4;

            var topicLength = BitConverter.ToInt32(topicLengthBytes, 0);
            var topicBytes = new byte[topicLength];
            Buffer.BlockCopy(messageBuffer, headerLength, topicBytes, 0, topicLength);
            headerLength += topicLength;

            Buffer.BlockCopy(messageBuffer, headerLength, routingKeyLengthBytes, 0, 4);
            headerLength += 4;

            var routingKeyLength = BitConverter.ToInt32(routingKeyLengthBytes, 0);
            var routingKeyBytes = new byte[routingKeyLength];
            Buffer.BlockCopy(messageBuffer, headerLength, routingKeyBytes, 0, routingKeyLength);
            headerLength += routingKeyLength;

            var bodyBytes = new byte[messageBuffer.Length - headerLength];
            Buffer.BlockCopy(messageBuffer, headerLength, bodyBytes, 0, bodyBytes.Length);

            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var code = BitConverter.ToInt32(codeBytes, 0);
            var topic = Encoding.UTF8.GetString(topicBytes);
            var routingKey = Encoding.UTF8.GetString(routingKeyBytes);

            return new SendMessageRequest { QueueId = queueId, Message = new Message(topic, code, bodyBytes), RoutingKey = routingKey };
        }

        private static byte[] Combine(params byte[][] arrays)
        {
            byte[] destination = new byte[arrays.Sum(x => x.Length)];
            int offset = 0;
            foreach (byte[] data in arrays)
            {
                Buffer.BlockCopy(data, 0, destination, offset, data.Length);
                offset += data.Length;
            }
            return destination;
        }
    }
}
