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
        private ConcurrentDictionary<string, IEnumerable<string>> _consumerSubscriptionTopicDict = new ConcurrentDictionary<string, IEnumerable<string>>();
        private ConcurrentDictionary<string, IEnumerable<string>> _consumerConsumingQueueDict = new ConcurrentDictionary<string, IEnumerable<string>>();
        private ILogger _logger;

        public string GroupName { get { return _groupName; } }

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
                _logger.InfoFormat("Consumer added into group. groupName:{0}, consumerId:{1}, remotingAddress:{2}", _groupName, clientChannel.ClientId, clientChannel.Channel.RemoteEndPoint.ToString());
                return clientChannel;
            }, (key, old) => clientChannel);
            consumer.LastUpdateTime = DateTime.Now;
        }
        public void UpdateConsumerSubscriptionTopics(ClientChannel clientChannel, IEnumerable<string> subscriptionTopics)
        {
            var subscriptionTopicChanged = false;
            IEnumerable<string> oldSubscriptionTopics = new List<string>();
            IEnumerable<string> newSubscriptionTopics = new List<string>();

            _consumerSubscriptionTopicDict.AddOrUpdate(clientChannel.ClientId,
            key =>
            {
                subscriptionTopicChanged = true;
                newSubscriptionTopics = subscriptionTopics;
                return subscriptionTopics;
            },
            (key, old) =>
            {
                if (IsStringCollectionChanged(old.ToList(), subscriptionTopics.ToList()))
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
        public void UpdateConsumerConsumingQueues(ClientChannel clientChannel, IEnumerable<string> consumingQueues)
        {
            var consumingQueueChanged = false;
            IEnumerable<string> oldConsumingQueues = new List<string>();
            IEnumerable<string> newConsumingQueues = new List<string>();

            _consumerConsumingQueueDict.AddOrUpdate(clientChannel.ClientId,
            key =>
            {
                newConsumingQueues = consumingQueues;
                if (consumingQueues.Count() > 0)
                {
                    consumingQueueChanged = true;
                }
                return consumingQueues;
            },
            (key, old) =>
            {
                if (IsStringCollectionChanged(old.ToList(), consumingQueues.ToList()))
                {
                    consumingQueueChanged = true;
                    oldConsumingQueues = old;
                    newConsumingQueues = consumingQueues;
                }
                return consumingQueues;
            });

            if (consumingQueueChanged)
            {
                _logger.InfoFormat("Consumer consuming queues changed. groupName:{0}, consumerId:{1}, old:{2}, new:{3}", _groupName, clientChannel.ClientId, string.Join("|", oldConsumingQueues), string.Join("|", newConsumingQueues));
            }
        }
        public bool IsConsumerActive(string consumerRemotingAddress)
        {
            return _consumerDict.Values.Any(x => x.Channel.RemoteEndPoint.ToString() == consumerRemotingAddress);
        }
        public void RemoveConsumer(string consumerRemotingAddress)
        {
            var clientChannel = _consumerDict.Values.SingleOrDefault(x => x.Channel.RemoteEndPoint.ToString() == consumerRemotingAddress);
            if (clientChannel != null)
            {
                ClientChannel currentClientChannel;
                if (_consumerDict.TryRemove(clientChannel.ClientId, out currentClientChannel))
                {
                    clientChannel.Close();

                    IEnumerable<string> subscriptionTopics;
                    if (!_consumerSubscriptionTopicDict.TryRemove(clientChannel.ClientId, out subscriptionTopics))
                    {
                        subscriptionTopics = new List<string>();
                    }
                    IEnumerable<string> consumingQueues;
                    if (!_consumerConsumingQueueDict.TryRemove(clientChannel.ClientId, out consumingQueues))
                    {
                        consumingQueues = new List<string>();
                    }
                    _logger.InfoFormat("Consumer removed from group. consumerGroup:{0}, consumerInfo:{1}, subscriptionTopics:{2}, consumingQueues:{3}", _groupName, clientChannel, string.Join("|", subscriptionTopics), string.Join("|", consumingQueues));
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
        public IEnumerable<string> GetAllConsumerIds()
        {
            return _consumerDict.Keys;
        }
        public IEnumerable<string> GetConsumerIdsForTopic(string topic)
        {
            return _consumerSubscriptionTopicDict.Where(x => x.Value.Any(y => y == topic)).Select(z => z.Key);
        }
        public IEnumerable<string> QueryConsumerIdsForTopic(string topic)
        {
            return _consumerSubscriptionTopicDict.Where(x => x.Value.Any(y => y.Contains(topic))).Select(z => z.Key);
        }
        public IEnumerable<string> GetConsumingQueue(string consumerId)
        {
            IEnumerable<string> consumingQueues;
            if (_consumerConsumingQueueDict.TryGetValue(consumerId, out consumingQueues))
            {
                return consumingQueues;
            }
            return new List<string>();
        }

        private bool IsStringCollectionChanged(IList<string> original, IList<string> current)
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
