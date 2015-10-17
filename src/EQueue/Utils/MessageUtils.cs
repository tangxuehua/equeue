using System;
using System.Linq;
using System.Text;
using EQueue.Clients.Producers;
using EQueue.Protocols;

namespace EQueue.Utils
{
    public class MessageUtils
    {
        public static byte[] EncodeSendMessageRequest(SendMessageRequest request)
        {
            //queueId
            var queueIdBytes = BitConverter.GetBytes(request.QueueId);

            //messageCode
            var messageCodeBytes = BitConverter.GetBytes(request.Message.Code);

            //createdTimeTicks
            var messageCreatedTimeTicksBytes = BitConverter.GetBytes(request.Message.CreatedTime.Ticks);

            //messageKey
            var keyBytes = Encoding.UTF8.GetBytes(request.Message.Key);
            var keyLengthBytes = BitConverter.GetBytes(keyBytes.Length);

            //topic
            var topicBytes = Encoding.UTF8.GetBytes(request.Message.Topic);
            var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);

            //routingKey
            var routingKeyBytes = Encoding.UTF8.GetBytes(request.RoutingKey);
            var routingKeyLengthBytes = BitConverter.GetBytes(routingKeyBytes.Length);

            return Combine(
                queueIdBytes,
                messageCodeBytes,
                messageCreatedTimeTicksBytes,
                keyLengthBytes,
                keyBytes,
                topicLengthBytes,
                topicBytes,
                routingKeyLengthBytes,
                routingKeyBytes,
                request.Message.Body);
        }
        public static SendMessageRequest DecodeSendMessageRequest(byte[] messageBuffer)
        {
            var queueIdBytes = new byte[4];
            var messageCodeBytes = new byte[4];
            var messageCreatedTimeTicksBytes = new byte[8];
            var topicLengthBytes = new byte[4];
            var routingKeyLengthBytes = new byte[4];
            var keyLengthBytes = new byte[4];
            var headerLength = 0;

            //queueId
            Buffer.BlockCopy(messageBuffer, 0, queueIdBytes, 0, 4);
            headerLength += 4;

            //messageCode
            Buffer.BlockCopy(messageBuffer, headerLength, messageCodeBytes, 0, 4);
            headerLength += 4;

            //messageCreatedTimeTicks
            Buffer.BlockCopy(messageBuffer, headerLength, messageCreatedTimeTicksBytes, 0, 8);
            headerLength += 8;

            //key
            Buffer.BlockCopy(messageBuffer, headerLength, keyLengthBytes, 0, 4);
            headerLength += 4;

            var keyLength = BitConverter.ToInt32(keyLengthBytes, 0);
            var keyBytes = new byte[keyLength];
            Buffer.BlockCopy(messageBuffer, headerLength, keyBytes, 0, keyLength);
            headerLength += keyLength;

            //topic
            Buffer.BlockCopy(messageBuffer, headerLength, topicLengthBytes, 0, 4);
            headerLength += 4;

            var topicLength = BitConverter.ToInt32(topicLengthBytes, 0);
            var topicBytes = new byte[topicLength];
            Buffer.BlockCopy(messageBuffer, headerLength, topicBytes, 0, topicLength);
            headerLength += topicLength;

            //routingKey
            Buffer.BlockCopy(messageBuffer, headerLength, routingKeyLengthBytes, 0, 4);
            headerLength += 4;

            var routingKeyLength = BitConverter.ToInt32(routingKeyLengthBytes, 0);
            var routingKeyBytes = new byte[routingKeyLength];
            Buffer.BlockCopy(messageBuffer, headerLength, routingKeyBytes, 0, routingKeyLength);
            headerLength += routingKeyLength;

            //body
            var bodyBytes = new byte[messageBuffer.Length - headerLength];
            Buffer.BlockCopy(messageBuffer, headerLength, bodyBytes, 0, bodyBytes.Length);

            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var code = BitConverter.ToInt32(messageCodeBytes, 0);
            var createdTimeTicks = BitConverter.ToInt64(messageCreatedTimeTicksBytes, 0);
            var createdTime = new DateTime(createdTimeTicks);
            var topic = Encoding.UTF8.GetString(topicBytes);
            var key = Encoding.UTF8.GetString(keyBytes);
            var routingKey = Encoding.UTF8.GetString(routingKeyBytes);

            return new SendMessageRequest { QueueId = queueId, Message = new Message(topic, code, key, bodyBytes, createdTime), RoutingKey = routingKey };
        }

        public static byte[] EncodeMessageSendResponse(SendMessageResponse response)
        {
            //messageOffset
            var messageOffsetBytes = BitConverter.GetBytes(response.MessageOffset);

            //code
            var messageCodeBytes = BitConverter.GetBytes(response.MessageCode);

            //queueId
            var queueIdBytes = BitConverter.GetBytes(response.QueueId);

            //queueOffset
            var queueOffsetBytes = BitConverter.GetBytes(response.QueueOffset);

            //messageId
            var messageIdBytes = Encoding.UTF8.GetBytes(response.MessageId);
            var messageIdLengthBytes = BitConverter.GetBytes(messageIdBytes.Length);

            //messageKey
            var messageKeyBytes = Encoding.UTF8.GetBytes(response.MessageKey);
            var messageKeyLengthBytes = BitConverter.GetBytes(messageKeyBytes.Length);

            //topic
            var topicBytes = Encoding.UTF8.GetBytes(response.Topic);
            var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);

            return Combine(
                messageOffsetBytes,
                messageCodeBytes,
                queueIdBytes,
                queueOffsetBytes,
                messageIdLengthBytes,
                messageIdBytes,
                messageKeyLengthBytes,
                messageKeyBytes,
                topicLengthBytes,
                topicBytes);
        }
        public static SendMessageResponse DecodeMessageSendResponse(byte[] buffer)
        {
            var messageOffsetBytes = new byte[8];
            var messageCodeBytes = new byte[4];
            var queueIdBytes = new byte[4];
            var queueOffsetBytes = new byte[8];
            var messageIdLengthBytes = new byte[4];
            var messageKeyLengthBytes = new byte[4];
            var topicLengthBytes = new byte[4];
            var headerLength = 0;

            //messageOffset
            Buffer.BlockCopy(buffer, 0, messageOffsetBytes, 0, 8);
            headerLength += 8;

            //messageCode
            Buffer.BlockCopy(buffer, 0, messageCodeBytes, 0, 4);
            headerLength += 4;

            //queueId
            Buffer.BlockCopy(buffer, 0, queueIdBytes, 0, 4);
            headerLength += 4;

            //queueOffset
            Buffer.BlockCopy(buffer, 0, queueOffsetBytes, 0, 8);
            headerLength += 8;

            //messageId
            Buffer.BlockCopy(buffer, headerLength, messageIdLengthBytes, 0, 4);
            headerLength += 4;

            var messageIdLength = BitConverter.ToInt32(messageIdLengthBytes, 0);
            var messageIdBytes = new byte[messageIdLength];
            Buffer.BlockCopy(buffer, headerLength, messageIdBytes, 0, messageIdLength);
            headerLength += messageIdLength;

            //messageKey
            Buffer.BlockCopy(buffer, headerLength, messageKeyLengthBytes, 0, 4);
            headerLength += 4;

            var messageKeyLength = BitConverter.ToInt32(messageKeyLengthBytes, 0);
            var messageKeyBytes = new byte[messageKeyLength];
            Buffer.BlockCopy(buffer, headerLength, messageKeyBytes, 0, messageKeyLength);
            headerLength += messageKeyLength;

            //topic
            Buffer.BlockCopy(buffer, headerLength, topicLengthBytes, 0, 4);
            headerLength += 4;

            var topicLength = BitConverter.ToInt32(topicLengthBytes, 0);
            var topicBytes = new byte[topicLength];
            Buffer.BlockCopy(buffer, headerLength, topicBytes, 0, topicLength);
            headerLength += topicLength;

            var messageOffset = BitConverter.ToInt64(messageOffsetBytes, 0);
            var messageCode = BitConverter.ToInt32(messageCodeBytes, 0);
            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var queueOffset = BitConverter.ToInt64(queueOffsetBytes, 0);
            var messageId = Encoding.UTF8.GetString(messageIdBytes);
            var messageKey = Encoding.UTF8.GetString(messageKeyBytes);
            var topic = Encoding.UTF8.GetString(topicBytes);

            return new SendMessageResponse(
                messageKey,
                messageId,
                messageOffset,
                messageCode,
                topic,
                queueId,
                queueOffset);
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
