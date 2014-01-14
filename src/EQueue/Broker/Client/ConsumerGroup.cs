using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;

namespace EQueue.Broker.Client
{
    public class ConsumerGroup
    {
        private const long ChannelExpiredTimeout = 1000 * 120;
        private string _groupName;
        private MessageModel _messageModel;
        private ConcurrentDictionary<string, ClientChannel> _consumerChannelDict = new ConcurrentDictionary<string, ClientChannel>();
        private ConcurrentDictionary<string, string> _subscriptionTopicDict = new ConcurrentDictionary<string, string>();
        private ILogger _logger;

        public DateTime LastUpdateTime { get; private set; }

        public ConsumerGroup(string groupName, MessageModel messageModel)
        {
            _groupName = groupName;
            _messageModel = messageModel;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public bool UpdateChannel(ClientChannel clientChannel, MessageModel messageModel)
        {
            var consumeGroupChanged = false;

            if (_messageModel != messageModel)
            {
                _messageModel = messageModel;
                consumeGroupChanged = true;
            }

            var currentChannel = _consumerChannelDict.GetOrAdd(clientChannel.Channel.RemotingAddress, x =>
            {
                consumeGroupChanged = true;
                return clientChannel;
            });

            var currentTime = DateTime.Now;
            LastUpdateTime = currentTime;
            currentChannel.LastUpdateTime = currentTime;

            return consumeGroupChanged;
        }
        public bool UpdateSubscriptionTopics(IEnumerable<string> subscriptionTopics)
        {
            var subscriptionTopicChanged = false;

            //Only care the new subscription topics of the current consumer group.
            foreach (var topic in subscriptionTopics)
            {
                if (_subscriptionTopicDict.TryAdd(topic, topic))
                {
                    subscriptionTopicChanged = true;
                }
            }
            LastUpdateTime = DateTime.Now;

            return subscriptionTopicChanged;
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
                _logger.WarnFormat("Removed not active consumer client channel from consumer group. consumer Group:{0}, clientChannel:{1}", _groupName, clientChannel);
            }
        }
        public void RemoteNotActiveConsumerChannels()
        {
            foreach (var entry in _consumerChannelDict)
            {
                var channelRemotingAddress = entry.Key;
                var clientChannel = entry.Value;
                if (DateTime.Now > clientChannel.LastUpdateTime.AddMilliseconds(ChannelExpiredTimeout))
                {
                    clientChannel.Channel.Close();
                    RemoveConsumerChannel(channelRemotingAddress);
                }
            }
        }

        public IEnumerable<ClientChannel> GetAllConsumerChannels()
        {
            return _consumerChannelDict.Values.ToArray();
        }
    }
}
