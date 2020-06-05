using System;
using System.Text;
using ECommon.Utilities;
using EQueue.Broker;

namespace EQueue.Utils
{
    public class MessageIdUtil
    {
        private static byte[] _brokerNameBytes;
        private static byte[] _brokerNameLengthBytes;

        public static string CreateMessageId(long messagePosition)
        {
            if (_brokerNameBytes == null)
            {
                _brokerNameBytes = Encoding.UTF8.GetBytes(BrokerController.Instance.Setting.BrokerInfo.BrokerName);
                _brokerNameLengthBytes = BitConverter.GetBytes(_brokerNameBytes.Length);
            }
            var positionBytes = BitConverter.GetBytes(messagePosition);
            var messageIdBytes = ByteUtil.Combine(_brokerNameLengthBytes, _brokerNameBytes, positionBytes);

            return ObjectId.ToHexString(messageIdBytes);
        }
        public static MessageIdInfo ParseMessageId(string messageId)
        {
            var messageIdBytes = ObjectId.ParseHexString(messageId);
            var brokerNameLengthBytes = new byte[4];
            var messagePositionBytes = new byte[8];

            Buffer.BlockCopy(messageIdBytes, 0, brokerNameLengthBytes, 0, 4);
            var brokerNameLength = BitConverter.ToInt32(brokerNameLengthBytes, 0);
            var brokerNameBytes = new byte[brokerNameLength];
            Buffer.BlockCopy(messageIdBytes, 4, brokerNameBytes, 0, brokerNameLength);
            Buffer.BlockCopy(messageIdBytes, 4 + brokerNameLength, messagePositionBytes, 0, 8);

            var brokerName = Encoding.UTF8.GetString(brokerNameBytes);
            var messagePosition = BitConverter.ToInt64(messagePositionBytes, 0);

            return new MessageIdInfo
            {
                BrokerName = brokerName,
                MessagePosition = messagePosition
            };
        }
    }
    public struct MessageIdInfo
    {
        public string BrokerName;
        public long MessagePosition;
    }
}
