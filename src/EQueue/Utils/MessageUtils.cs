using System;
using System.Text;
using ECommon.Utilities;
using EQueue.Protocols;
using EQueue.Protocols.Brokers.Requests;

namespace EQueue.Utils
{
    public class MessageUtils
    {
        private static readonly byte[] EmptyBytes = new byte[0];

        public static byte[] EncodeSendMessageRequest(SendMessageRequest request)
        {
            //queueId
            var queueIdBytes = BitConverter.GetBytes(request.QueueId);

            //messageCode
            var messageCodeBytes = BitConverter.GetBytes(request.Message.Code);

            //createdTimeTicks
            var messageCreatedTimeTicksBytes = BitConverter.GetBytes(request.Message.CreatedTime.Ticks);

            //topic
            var topicBytes = Encoding.UTF8.GetBytes(request.Message.Topic);
            var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);

            //tag
            var tagBytes = EmptyBytes;
            if (!string.IsNullOrEmpty(request.Message.Tag))
            {
                tagBytes = Encoding.UTF8.GetBytes(request.Message.Tag);
            }
            var tagLengthBytes = BitConverter.GetBytes(tagBytes.Length);

            //producerAddress
            var producerAddressBytes = Encoding.UTF8.GetBytes(request.ProducerAddress);
            var producerAddressLengthBytes = BitConverter.GetBytes(producerAddressBytes.Length);

            return ByteUtil.Combine(
                queueIdBytes,
                messageCodeBytes,
                messageCreatedTimeTicksBytes,
                topicLengthBytes,
                topicBytes,
                tagLengthBytes,
                tagBytes,
                producerAddressLengthBytes,
                producerAddressBytes,
                request.Message.Body);
        }
        public static SendMessageRequest DecodeSendMessageRequest(byte[] messageBuffer)
        {
            var queueIdBytes = new byte[4];
            var messageCodeBytes = new byte[4];
            var messageCreatedTimeTicksBytes = new byte[8];
            var topicLengthBytes = new byte[4];
            var tagLengthBytes = new byte[4];
            var producerAddressLengthBytes = new byte[4];
            var srcOffset = 0;

            //queueId
            Buffer.BlockCopy(messageBuffer, srcOffset, queueIdBytes, 0, 4);
            srcOffset += 4;

            //messageCode
            Buffer.BlockCopy(messageBuffer, srcOffset, messageCodeBytes, 0, 4);
            srcOffset += 4;

            //messageCreatedTimeTicks
            Buffer.BlockCopy(messageBuffer, srcOffset, messageCreatedTimeTicksBytes, 0, 8);
            srcOffset += 8;

            //topic
            Buffer.BlockCopy(messageBuffer, srcOffset, topicLengthBytes, 0, 4);
            srcOffset += 4;

            var topicLength = BitConverter.ToInt32(topicLengthBytes, 0);
            var topicBytes = new byte[topicLength];
            Buffer.BlockCopy(messageBuffer, srcOffset, topicBytes, 0, topicLength);
            srcOffset += topicLength;

            //tag
            Buffer.BlockCopy(messageBuffer, srcOffset, tagLengthBytes, 0, 4);
            srcOffset += 4;

            var tagLength = BitConverter.ToInt32(tagLengthBytes, 0);
            var tagBytes = new byte[tagLength];
            Buffer.BlockCopy(messageBuffer, srcOffset, tagBytes, 0, tagLength);
            srcOffset += tagLength;

            //producerAddress
            Buffer.BlockCopy(messageBuffer, srcOffset, producerAddressLengthBytes, 0, 4);
            srcOffset += 4;

            var producerAddressLength = BitConverter.ToInt32(producerAddressLengthBytes, 0);
            var producerAddressBytes = new byte[producerAddressLength];
            Buffer.BlockCopy(messageBuffer, srcOffset, producerAddressBytes, 0, producerAddressLength);
            srcOffset += producerAddressLength;

            //body
            var bodyBytes = new byte[messageBuffer.Length - srcOffset];
            Buffer.BlockCopy(messageBuffer, srcOffset, bodyBytes, 0, bodyBytes.Length);

            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var code = BitConverter.ToInt32(messageCodeBytes, 0);
            var createdTimeTicks = BitConverter.ToInt64(messageCreatedTimeTicksBytes, 0);
            var createdTime = new DateTime(createdTimeTicks);
            var topic = Encoding.UTF8.GetString(topicBytes);
            var tag = Encoding.UTF8.GetString(tagBytes);
            var producerAddress = Encoding.UTF8.GetString(producerAddressBytes);

            return new SendMessageRequest { QueueId = queueId, Message = new Message(topic, code, bodyBytes, createdTime, tag), ProducerAddress = producerAddress };
        }

        public static byte[] EncodeMessageStoreResult(MessageStoreResult result)
        {
            //code
            var messageCodeBytes = BitConverter.GetBytes(result.Code);

            //queueId
            var queueIdBytes = BitConverter.GetBytes(result.QueueId);

            //queueOffset
            var queueOffsetBytes = BitConverter.GetBytes(result.QueueOffset);

            //messageId
            var messageIdBytes = Encoding.UTF8.GetBytes(result.MessageId);
            var messageIdLengthBytes = BitConverter.GetBytes(messageIdBytes.Length);

            //topic
            var topicBytes = Encoding.UTF8.GetBytes(result.Topic);
            var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);

            //tag
            var tagBytes = EmptyBytes;
            if (!string.IsNullOrEmpty(result.Tag))
            {
                tagBytes = Encoding.UTF8.GetBytes(result.Tag);
            }
            var tagLengthBytes = BitConverter.GetBytes(tagBytes.Length);

            //createdTimeTicks
            var createdTimeTicksBytes = BitConverter.GetBytes(result.CreatedTime.Ticks);
            //storedTimeTicks
            var storedTimeTicksBytes = BitConverter.GetBytes(result.StoredTime.Ticks);

            return ByteUtil.Combine(
                messageCodeBytes,
                queueIdBytes,
                queueOffsetBytes,
                messageIdLengthBytes,
                messageIdBytes,
                topicLengthBytes,
                topicBytes,
                tagLengthBytes,
                tagBytes,
                createdTimeTicksBytes,
                storedTimeTicksBytes);
        }
        public static MessageStoreResult DecodeMessageStoreResult(byte[] buffer)
        {
            var messageCodeBytes = new byte[4];
            var queueIdBytes = new byte[4];
            var queueOffsetBytes = new byte[8];
            var messageIdLengthBytes = new byte[4];
            var topicLengthBytes = new byte[4];
            var tagLengthBytes = new byte[4];
            var createdTimeTicksBytes = new byte[8];
            var storedTimeTicksBytes = new byte[8];
            var srcOffset = 0;

            //messageCode
            Buffer.BlockCopy(buffer, srcOffset, messageCodeBytes, 0, 4);
            srcOffset += 4;

            //queueId
            Buffer.BlockCopy(buffer, srcOffset, queueIdBytes, 0, 4);
            srcOffset += 4;

            //queueOffset
            Buffer.BlockCopy(buffer, srcOffset, queueOffsetBytes, 0, 8);
            srcOffset += 8;

            //messageId
            Buffer.BlockCopy(buffer, srcOffset, messageIdLengthBytes, 0, 4);
            srcOffset += 4;

            var messageIdLength = BitConverter.ToInt32(messageIdLengthBytes, 0);
            var messageIdBytes = new byte[messageIdLength];
            Buffer.BlockCopy(buffer, srcOffset, messageIdBytes, 0, messageIdLength);
            srcOffset += messageIdLength;

            //topic
            Buffer.BlockCopy(buffer, srcOffset, topicLengthBytes, 0, 4);
            srcOffset += 4;

            var topicLength = BitConverter.ToInt32(topicLengthBytes, 0);
            var topicBytes = new byte[topicLength];
            Buffer.BlockCopy(buffer, srcOffset, topicBytes, 0, topicLength);
            srcOffset += topicLength;

            //tag
            Buffer.BlockCopy(buffer, srcOffset, tagLengthBytes, 0, 4);
            srcOffset += 4;

            var tagLength = BitConverter.ToInt32(tagLengthBytes, 0);
            var tagBytes = new byte[tagLength];
            Buffer.BlockCopy(buffer, srcOffset, tagBytes, 0, tagLength);
            srcOffset += tagLength;

            //createdTime
            Buffer.BlockCopy(buffer, srcOffset, createdTimeTicksBytes, 0, 8);
            srcOffset += 8;

            //storedTime
            Buffer.BlockCopy(buffer, srcOffset, storedTimeTicksBytes, 0, 8);
            srcOffset += 8;

            var messageId = Encoding.UTF8.GetString(messageIdBytes);
            var code = BitConverter.ToInt32(messageCodeBytes, 0);
            var topic = Encoding.UTF8.GetString(topicBytes);
            var tag = Encoding.UTF8.GetString(tagBytes);
            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var queueOffset = BitConverter.ToInt64(queueOffsetBytes, 0);
            var createdTimeTicks = BitConverter.ToInt64(createdTimeTicksBytes, 0);
            var createdTime = new DateTime(createdTimeTicks);
            var storedTimeTicks = BitConverter.ToInt64(storedTimeTicksBytes, 0);
            var storedTime = new DateTime(storedTimeTicks);

            return new MessageStoreResult(
                messageId,
                code,
                topic,
                queueId,
                queueOffset,
                createdTime,
                storedTime,
                tag);
        }    
    }
}
