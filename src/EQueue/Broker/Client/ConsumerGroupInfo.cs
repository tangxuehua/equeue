using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using EQueue.Protocols;
using EQueue.Remoting;

namespace EQueue.Broker.Client
{
    public class ConsumerGroupInfo
    {
        private string _groupName;
        private MessageModel _messageModel;
        private ConcurrentDictionary<IChannel, ClientChannelInfo> _clientChannelDict = new ConcurrentDictionary<IChannel, ClientChannelInfo>();

        public ConsumerGroupInfo(string groupName, MessageModel messageModel)
        {
            _groupName = groupName;
            _messageModel = messageModel;
        }

        public IEnumerable<IChannel> GetAllChannels()
        {
            return _clientChannelDict.Keys.ToArray();
        }
        public IEnumerable<string> GetAllClientIds()
        {
            return _clientChannelDict.Values.Select(x => x.ClientId).ToArray();
        }
    }
}
