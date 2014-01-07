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
    public class ConsumerGroupInfo
    {
        private const long ChannelExpiredTimeout = 1000 * 120;
        private string _groupName;
        private MessageModel _messageModel;
        private ConcurrentDictionary<string, ClientChannelInfo> _consumerChannelDict = new ConcurrentDictionary<string, ClientChannelInfo>();
        private ConcurrentDictionary<string, string> _subscriptionTopicDict = new ConcurrentDictionary<string, string>();
        private ILogger _logger;

        public DateTime LastUpdateTime { get; private set; }

        public ConsumerGroupInfo(string groupName, MessageModel messageModel)
        {
            _groupName = groupName;
            _messageModel = messageModel;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public bool UpdateChannel(ClientChannelInfo clientChannelInfo, MessageModel messageModel)
        {
            var consumeGroupChanged = false;

            if (_messageModel != messageModel)
            {
                _messageModel = messageModel;
                consumeGroupChanged = true;
            }

            var channelInfo = _consumerChannelDict.GetOrAdd(clientChannelInfo.ClientId, x =>
            {
                consumeGroupChanged = true;
                return clientChannelInfo;
            });

            var currentTime = DateTime.Now;
            LastUpdateTime = currentTime;
            channelInfo.LastUpdateTime = currentTime;

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
        public void RemoteClientChannel(string clientId)
        {
            ClientChannelInfo channel;
            if (_consumerChannelDict.TryRemove(clientId, out channel))
            {
                _logger.WarnFormat("Removed not active channel from ConsumerGroupInfo. consumer Group:{0}, channel:{1}", _groupName, channel);
            }
        }
        public void RemoteNotActiveChannels()
        {
            foreach (var entry in _consumerChannelDict)
            {
                var clientId = entry.Key;
                var channelInfo = entry.Value;
                if (DateTime.Now > channelInfo.LastUpdateTime.AddMilliseconds(ChannelExpiredTimeout))
                {
                    channelInfo.Channel.Close();
                    RemoteClientChannel(clientId);
                }
            }
        }

        public IEnumerable<ClientChannelInfo> GetAllConsumerChannels()
        {
            return _consumerChannelDict.Values.ToArray();
        }
    }
}
