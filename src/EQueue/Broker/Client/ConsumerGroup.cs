using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Socketing;
using EQueue.Utils;

namespace EQueue.Broker.Client
{
    public class ConsumerGroup
    {
        private readonly string _groupName;
        private readonly ConcurrentDictionary<string, ConsumerHeartbeatInfo> _consumerDict = new ConcurrentDictionary<string, ConsumerHeartbeatInfo>();
        private readonly ConcurrentDictionary<string, IEnumerable<string>> _consumerSubscriptionTopicDict = new ConcurrentDictionary<string, IEnumerable<string>>();
        private readonly ConcurrentDictionary<string, IEnumerable<string>> _consumerConsumingQueueDict = new ConcurrentDictionary<string, IEnumerable<string>>();
        private readonly ILogger _logger;

        public string GroupName { get { return _groupName; } }

        public ConsumerGroup(string groupName)
        {
            _groupName = groupName;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Register(string consumerId, ITcpConnection connection)
        {
            var consumerHeartbeatInfo = _consumerDict.GetOrAdd(consumerId, key =>
            {
                _logger.InfoFormat("Consumer registered to group: {0}, consumerId: {1}", _groupName, consumerId);
                return new ConsumerHeartbeatInfo(key, connection);
            });
            consumerHeartbeatInfo.LastHeartbeatTime = DateTime.Now;
        }
        public void UpdateConsumerSubscriptionTopics(string consumerId, IEnumerable<string> subscriptionTopics)
        {
            var subscriptionTopicChanged = false;
            IEnumerable<string> oldSubscriptionTopics = new List<string>();
            IEnumerable<string> newSubscriptionTopics = new List<string>();

            _consumerSubscriptionTopicDict.AddOrUpdate(consumerId,
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
                _logger.InfoFormat("Consumer subscription topics changed. groupName:{0}, consumerId:{1}, old:{2}, new:{3}", _groupName, consumerId, string.Join("|", oldSubscriptionTopics), string.Join("|", newSubscriptionTopics));
            }
        }
        public void UpdateConsumerConsumingQueues(string consumerId, IEnumerable<string> consumingQueues)
        {
            var consumingQueueChanged = false;
            IEnumerable<string> oldConsumingQueues = new List<string>();
            IEnumerable<string> newConsumingQueues = new List<string>();

            _consumerConsumingQueueDict.AddOrUpdate(consumerId,
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
                _logger.InfoFormat("Consumer consuming queues changed. groupName:{0}, consumerId:{1}, old:{2}, new:{3}", _groupName, consumerId, string.Join("|", oldConsumingQueues), string.Join("|", newConsumingQueues));
            }
        }
        public bool IsConsumerActive(string consumerId)
        {
            return _consumerDict.ContainsKey(consumerId);
        }
        public void RemoveConsumer(string consumerId)
        {
            ConsumerHeartbeatInfo consumerHeartbeatInfo;
            if (_consumerDict.TryRemove(consumerId, out consumerHeartbeatInfo))
            {
                try
                {
                    consumerHeartbeatInfo.Connection.Close();
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Close tcp connection for consumer client failed, consumerId: {0}", consumerId), ex);
                }

                IEnumerable<string> subscriptionTopics;
                if (!_consumerSubscriptionTopicDict.TryRemove(consumerHeartbeatInfo.ConsumerId, out subscriptionTopics))
                {
                    subscriptionTopics = new List<string>();
                }
                IEnumerable<string> consumingQueues;
                if (!_consumerConsumingQueueDict.TryRemove(consumerHeartbeatInfo.ConsumerId, out consumingQueues))
                {
                    consumingQueues = new List<string>();
                }
                _logger.InfoFormat("Consumer removed from group: {0}, heartbeatInfo: {1}, subscriptionTopics: {2}, consumingQueues: {3}", _groupName, consumerHeartbeatInfo, string.Join("|", subscriptionTopics), string.Join("|", consumingQueues));
            }
        }
        public void RemoveNotActiveConsumers()
        {
            foreach (var consumerHeartbeatInfo in _consumerDict.Values)
            {
                if (consumerHeartbeatInfo.IsTimeout(BrokerController.Instance.Setting.ConsumerExpiredTimeout))
                {
                    RemoveConsumer(consumerHeartbeatInfo.ConsumerId);
                }
            }
        }
        public IEnumerable<string> GetAllConsumerIds()
        {
            return _consumerDict.Keys;
        }
        public int GetConsumerCount()
        {
            return _consumerDict.Count;
        }
        public IEnumerable<string> GetConsumerIdsForTopic(string topic)
        {
            return _consumerSubscriptionTopicDict.Where(x => x.Value.Any(y => y == topic)).Select(z => z.Key);
        }
        public IEnumerable<string> QueryConsumerIdsForTopic(string topic)
        {
            return _consumerSubscriptionTopicDict.Where(x => x.Value.Any(y => y.Contains(topic))).Select(z => z.Key);
        }
        public bool IsConsumerExistForQueue(string topic, int queueId)
        {
            var key = QueueKeyUtil.CreateQueueKey(topic, queueId);
            return _consumerConsumingQueueDict.Values.Any(x => x.Any(y => y == key));
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
