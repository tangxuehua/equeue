using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;

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
            var subscriptionTopicChanged = false;
            IEnumerable<string> oldSubscriptionTopics = new List<string>();
            IEnumerable<string> newSubscriptionTopics = new List<string>();

            _clientSubscriptionTopicDict.AddOrUpdate(clientChannel.ClientId,
            key =>
            {
                subscriptionTopicChanged = true;
                newSubscriptionTopics = subscriptionTopics;
                return subscriptionTopics;
            },
            (key, old) =>
            {
                if (IsSubscriptionTopicsChanged(old.ToList(), subscriptionTopics.ToList()))
                {
                    subscriptionTopicChanged = true;
                    oldSubscriptionTopics = old;
                    newSubscriptionTopics = subscriptionTopics;
                }
                return subscriptionTopics;
            });

            if (subscriptionTopicChanged)
            {
                _logger.InfoFormat("Consumer subscription topics changed. groupName:{0}, consumerId:{1}, old:{2}, new:{3}", _groupName, clientChannel.ClientId, string.Join("|", oldSubscriptionTopics), string.Join("|", newSubscriptionTopics));
            }
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
                ClientChannel currentClientChannel;
                if (_consumerDict.TryRemove(clientChannel.ClientId, out currentClientChannel))
                {
                    clientChannel.Close();

                    IEnumerable<string> subscriptionTopics;
                    if (!_clientSubscriptionTopicDict.TryRemove(clientChannel.ClientId, out subscriptionTopics))
                    {
                        subscriptionTopics = new List<string>();
                    }
                    _logger.InfoFormat("Consumer removed from group. consumerGroup:{0}, consumerInfo:{1}, subscriptionTopics:{2}", _groupName, clientChannel, string.Join("|", subscriptionTopics));
                }
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

        private bool IsSubscriptionTopicsChanged(IList<string> original, IList<string> current)
        {
            if (original.Count != current.Count)
            {
                return true;
            }
            for (var index = 0; index < original.Count; index++)
            {
                if (original[index] != current[index])
                {
                    return true;
                }
            }
            return false;
        }
    }
}
