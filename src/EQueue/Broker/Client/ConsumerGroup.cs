using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using EQueue.Protocols;

namespace EQueue.Broker.Client
{
    public class ConsumerGroup
    {
        private const long ChannelExpiredTimeout = 1000 * 120;
        private string _groupName;
        private ConcurrentDictionary<string, ClientChannel> _consumerChannelDict = new ConcurrentDictionary<string, ClientChannel>();
        private ConcurrentDictionary<string, IEnumerable<string>> _clientSubscriptionTopicDict = new ConcurrentDictionary<string, IEnumerable<string>>();
        private ILogger _logger;

        public ConsumerGroup(string groupName)
        {
            _groupName = groupName;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void GetOrAddChannel(ClientChannel clientChannel)
        {
            var currentChannel = _consumerChannelDict.GetOrAdd(clientChannel.Channel.RemotingAddress, key =>
            {
                _logger.DebugFormat("Added consumer client into consumer group. consumerGroup:{0}, consumerId:{1}, channel:{2}", _groupName, clientChannel.ClientId, clientChannel.Channel);
                return clientChannel;
            });
            currentChannel.LastUpdateTime = DateTime.Now;
        }
        public void UpdateChannelSubscriptionTopics(ClientChannel clientChannel, IEnumerable<string> subscriptionTopics)
        {
            _clientSubscriptionTopicDict.AddOrUpdate(clientChannel.ClientId, subscriptionTopics, (key, old) => subscriptionTopics);
        }
        public bool IsConsumerChannelActive(string consumerChannelRemotingAddress)
        {
            ClientChannel clientChannel;
            if (_consumerChannelDict.TryGetValue(consumerChannelRemotingAddress, out clientChannel))
            {
                return true;
            }
            return false;
        }
        public void RemoveConsumerChannel(string consumerChannelRemotingAddress)
        {
            ClientChannel clientChannel;
            if (_consumerChannelDict.TryRemove(consumerChannelRemotingAddress, out clientChannel))
            {
                clientChannel.Close();
                _logger.DebugFormat("Removed consumer client from consumer group. consumerGroup:{0}, consumerInfo:{1}", _groupName, clientChannel);
            }
        }
        public void RemoteNotActiveConsumerChannels()
        {
            foreach (var entry in _consumerChannelDict)
            {
                var channelRemotingAddress = entry.Key;
                var clientChannel = entry.Value;
                if (clientChannel.IsTimeout(ChannelExpiredTimeout))
                {
                    RemoveConsumerChannel(channelRemotingAddress);
                }
            }
        }

        public IEnumerable<string> GetConsumerIdsForTopic(string topic)
        {
            return _clientSubscriptionTopicDict.Where(x => x.Value.Any(y => y == topic)).Select(z => z.Key);
        }
    }
}
