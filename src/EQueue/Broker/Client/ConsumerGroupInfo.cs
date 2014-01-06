using System;
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
        private ConcurrentDictionary<string, ClientChannelInfo> _consumerChannelDict = new ConcurrentDictionary<string, ClientChannelInfo>();
        private ConcurrentDictionary<string, string> _subscriptionTopicDict = new ConcurrentDictionary<string, string>();
        public DateTime LastUpdateTime { get; private set; }

        public ConsumerGroupInfo(string groupName, MessageModel messageModel)
        {
            _groupName = groupName;
            _messageModel = messageModel;
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

        public IEnumerable<ClientChannelInfo> GetAllConsumerChannels()
        {
            return _consumerChannelDict.Values.ToArray();
        }
    }
}
