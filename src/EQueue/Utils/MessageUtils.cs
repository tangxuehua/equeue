using System;
using System.Text;
using EQueue.Protocols;

namespace EQueue.Utils
{
    public class MessageUtils
    {
        public static byte[] EncodeSendMessageRequest(SendMessageRequest request)
        {
            var queueIdBytes = BitConverter.GetBytes(request.QueueId);
            var topicBytes = Encoding.UTF8.GetBytes(request.Message.Topic);
            var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);

            var data = new byte[queueIdBytes.Length + topicLengthBytes.Length + topicBytes.Length + request.Message.Body.Length];

            queueIdBytes.CopyTo(data, 0);
            topicLengthBytes.CopyTo(data, queueIdBytes.Length);
            topicBytes.CopyTo(data, queueIdBytes.Length + topicLengthBytes.Length);
            request.Message.Body.CopyTo(data, queueIdBytes.Length + topicLengthBytes.Length + topicBytes.Length);

            return data;
        }
        public static SendMessageRequest DecodeSendMessageRequest(byte[] messageBuffer)
        {
            var queueIdBytes = new byte[4];
            var topicLengthBytes = new byte[4];
            Array.Copy(messageBuffer, 0, queueIdBytes, 0, 4);
            Array.Copy(messageBuffer, 4, topicLengthBytes, 0, 4);

            var topicLength = BitConverter.ToInt32(topicLengthBytes, 0);
            var topicBytes = new byte[topicLength];
            var headerLength = 8 + topicLength;
            var bodyBytes = new byte[messageBuffer.Length - headerLength];

            Array.Copy(messageBuffer, 8, topicBytes, 0, topicLength);
            Array.Copy(messageBuffer, headerLength, bodyBytes, 0, bodyBytes.Length);

            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var topic = Encoding.UTF8.GetString(topicBytes);

            return new SendMessageRequest { QueueId = queueId, Message = new Message(topic, bodyBytes) };
        }
    }
}
