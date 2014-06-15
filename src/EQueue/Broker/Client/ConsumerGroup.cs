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
        private string _groupName;
        private ConsumerManager _consumerManager;
        private ConcurrentDictionary<string, ClientChannel> _consumerDict = new ConcurrentDictionary<string, ClientChannel>();
        private ConcurrentDictionary<string, IEnumerable<string>> _clientSubscriptionTopicDict = new ConcurrentDictionary<string, IEnumerable<string>>();
        private ILogger _logger;

        public ConsumerGroup(string groupName, ConsumerManager consumerManager)
        {
            _groupName = groupName;
            _consumerManager = consumerManager;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Register(ClientChannel clientChannel)
        {
            var consumer = _consumerDict.AddOrUpdate(clientChannel.ClientId, key =>
            {
                _logger.InfoFormat("Consumer added into group. groupName:{0}, consumerId:{1}, remotingAddress:{2}", _groupName, clientChannel.ClientId, clientChannel.Channel.RemotingAddress);
                return clientChannel;
            }, (key, old) => clientChannel);
            consumer.LastUpdateTime = DateTime.Now;
        }
        public void UpdateChannelSubscriptionTopics(ClientChannel clientChannel, IEnumerable<string> subscriptionTopics)
        {
            _clientSubscriptionTopicDict.AddOrUpdate(clientChannel.ClientId, subscriptionTopics, (key, old) => subscriptionTopics);
        }
        public bool IsConsumerActive(string consumerRemotingAddress)
        {
            return _consumerDict.Values.Any(x => x.Channel.RemotingAddress == consumerRemotingAddress);
        }
        public void RemoveConsumer(string consumerRemotingAddress)
        {
            var clientChannel = _consumerDict.Values.SingleOrDefault(x => x.Channel.RemotingAddress == consumerRemotingAddress);
            if (clientChannel != null)
            {
                clientChannel.Close();
                _logger.InfoFormat("Consume removed from group. groupName:{0}, consumerInfo:{1}", _groupName, clientChannel);
            }
        }
        public void RemoveNotActiveConsumers()
        {
            foreach (var entry in _consumerDict)
            {
                var channelRemotingAddress = entry.Key;
                var clientChannel = entry.Value;
                if (clientChannel.IsTimeout(_consumerManager.BrokerController.Setting.ConsumerExpiredTimeout))
                {
                    RemoveConsumer(channelRemotingAddress);
                }
            }
        }
        public IEnumerable<string> GetConsumerIdsForTopic(string topic)
        {
            return _clientSubscriptionTopicDict.Where(x => x.Value.Any(y => y == topic)).Select(z => z.Key);
        }
    }
}
