using System;
using System.Linq;
using System.Text;
using EQueue.Clients.Producers;
using EQueue.Protocols;

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

            return Combine(
                queueIdBytes,
                messageCodeBytes,
                messageCreatedTimeTicksBytes,
                topicLengthBytes,
                topicBytes,
                tagLengthBytes,
                tagBytes,
                request.Message.Body);
        }
        public static SendMessageRequest DecodeSendMessageRequest(byte[] messageBuffer)
        {
            var queueIdBytes = new byte[4];
            var messageCodeBytes = new byte[4];
            var messageCreatedTimeTicksBytes = new byte[8];
            var topicLengthBytes = new byte[4];
            var tagLengthBytes = new byte[4];
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

            //body
            var bodyBytes = new byte[messageBuffer.Length - srcOffset];
            Buffer.BlockCopy(messageBuffer, srcOffset, bodyBytes, 0, bodyBytes.Length);

            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var code = BitConverter.ToInt32(messageCodeBytes, 0);
            var createdTimeTicks = BitConverter.ToInt64(messageCreatedTimeTicksBytes, 0);
            var createdTime = new DateTime(createdTimeTicks);
            var topic = Encoding.UTF8.GetString(topicBytes);
            var tag = Encoding.UTF8.GetString(tagBytes);

            return new SendMessageRequest { QueueId = queueId, Message = new Message(topic, code, bodyBytes, createdTime, tag) };
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

            return Combine(
                messageCodeBytes,
                queueIdBytes,
                queueOffsetBytes,
                messageIdLengthBytes,
                messageIdBytes,
                topicLengthBytes,
                topicBytes,
                tagLengthBytes,
                tagBytes);
        }
        public static MessageStoreResult DecodeMessageStoreResult(byte[] buffer)
        {
            var messageCodeBytes = new byte[4];
            var queueIdBytes = new byte[4];
            var queueOffsetBytes = new byte[8];
            var messageIdLengthBytes = new byte[4];
            var topicLengthBytes = new byte[4];
            var tagLengthBytes = new byte[4];
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

            var messageId = Encoding.UTF8.GetString(messageIdBytes);
            var code = BitConverter.ToInt32(messageCodeBytes, 0);
            var topic = Encoding.UTF8.GetString(topicBytes);
            var tag = Encoding.UTF8.GetString(tagBytes);
            var queueId = BitConverter.ToInt32(queueIdBytes, 0);
            var queueOffset = BitConverter.ToInt64(queueOffsetBytes, 0);

            return new MessageStoreResult(
                messageId,
                code,
                topic,
                queueId,
                queueOffset,
                tag);
        }

        public static string DecodeString(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            return Encoding.UTF8.GetString(DecodeBytes(sourceBuffer, startOffset, out nextStartOffset));
        }
        public static int DecodeInt(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            var intBytes = new byte[4];
            Buffer.BlockCopy(sourceBuffer, startOffset, intBytes, 0, 4);
            nextStartOffset = startOffset + 4;
            return BitConverter.ToInt32(intBytes, 0);
        }
        public static long DecodeLong(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            var longBytes = new byte[8];
            Buffer.BlockCopy(sourceBuffer, startOffset, longBytes, 0, 8);
            nextStartOffset = startOffset + 8;
            return BitConverter.ToInt64(longBytes, 0);
        }
        public static DateTime DecodeDateTime(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            var longBytes = new byte[8];
            Buffer.BlockCopy(sourceBuffer, startOffset, longBytes, 0, 8);
            nextStartOffset = startOffset + 8;
            return new DateTime(BitConverter.ToInt64(longBytes, 0));
        }
        public static byte[] DecodeBytes(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            var lengthBytes = new byte[4];
            Buffer.BlockCopy(sourceBuffer, startOffset, lengthBytes, 0, 4);
            startOffset += 4;

            var length = BitConverter.ToInt32(lengthBytes, 0);
            var dataBytes = new byte[length];
            Buffer.BlockCopy(sourceBuffer, startOffset, dataBytes, 0, length);
            startOffset += length;

            nextStartOffset = startOffset;

            return dataBytes;
        }
        public static byte[] Combine(params byte[][] arrays)
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
