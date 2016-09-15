using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Socketing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.Client
{
    public class ConsumerGroup
    {
        class ConsumerInfo
        {
            public string ConsumerId;
            public ClientHeartbeatInfo HeartbeatInfo;
            public IList<string> SubscriptionTopics = new List<string>();
            public IList<QueueKey> ConsumingQueues = new List<QueueKey>();
        }
        private readonly string _groupName;
        private readonly ConcurrentDictionary<string /*connectionId*/, ConsumerInfo> _consumerInfoDict = new ConcurrentDictionary<string, ConsumerInfo>();
        private readonly ILogger _logger;

        public string GroupName { get { return _groupName; } }

        public ConsumerGroup(string groupName)
        {
            _groupName = groupName;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void RegisterConsumer(ITcpConnection connection, string consumerId, IList<string> subscriptionTopics, IList<MessageQueue> consumingMessageQueues)
        {
            var connectionId = connection.RemotingEndPoint.ToAddress();
            var consumingQueues = consumingMessageQueues.Select(x => new QueueKey(x.Topic, x.QueueId)).ToList();

            _consumerInfoDict.AddOrUpdate(connectionId, key =>
            {
                var newConsumerInfo = new ConsumerInfo
                {
                    ConsumerId = consumerId,
                    HeartbeatInfo = new ClientHeartbeatInfo(connection) { LastHeartbeatTime = DateTime.Now },
                    SubscriptionTopics = subscriptionTopics,
                    ConsumingQueues = consumingQueues
                };
                _logger.InfoFormat("Consumer registered to group, groupName: {0}, consumerId: {1}, connectionId: {2}, subscriptionTopics: {3}, consumingQueues: {4}", _groupName, consumerId, key, string.Join("|", subscriptionTopics), string.Join("|", consumingQueues));
                return newConsumerInfo;
            },
            (key, existingConsumerInfo) =>
            {
                existingConsumerInfo.HeartbeatInfo.LastHeartbeatTime = DateTime.Now;

                var oldSubscriptionList = existingConsumerInfo.SubscriptionTopics.ToList();
                var newSubscriptionList = subscriptionTopics.ToList();
                if (IsStringCollectionChanged(oldSubscriptionList, newSubscriptionList))
                {
                    existingConsumerInfo.SubscriptionTopics = newSubscriptionList;
                    _logger.InfoFormat("Consumer subscriptionTopics changed. groupName: {0}, consumerId: {1}, connectionId: {2}, old: {3}, new: {4}", _groupName, consumerId, key, string.Join("|", oldSubscriptionList), string.Join("|", newSubscriptionList));
                }

                var oldConsumingQueues = existingConsumerInfo.ConsumingQueues;
                var newConsumingQueues = consumingQueues;
                var oldList = oldConsumingQueues.Select(x => x.ToString()).ToList();
                var newList = newConsumingQueues.Select(x => x.ToString()).ToList();
                if (IsStringCollectionChanged(oldList, newList))
                {
                    existingConsumerInfo.ConsumingQueues = newConsumingQueues;
                    _logger.InfoFormat("Consumer consumingQueues changed. groupName: {0}, consumerId: {1}, connectionId: {2}, old: {3}, new: {4}", _groupName, consumerId, key, string.Join("|", oldConsumingQueues), string.Join("|", newConsumingQueues));
                }

                return existingConsumerInfo;
            });
        }
        public bool IsConsumerActive(string consumerId)
        {
            return _consumerInfoDict.Values.Any(x => x.ConsumerId == consumerId);
        }
        public void RemoveConsumer(string connectionId)
        {
            ConsumerInfo consumerInfo;
            if (_consumerInfoDict.TryRemove(connectionId, out consumerInfo))
            {
                try
                {
                    consumerInfo.HeartbeatInfo.Connection.Close();
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Close connection for consumer failed, consumerId: {0}, connectionId: {1}", consumerInfo.ConsumerId, connectionId), ex);
                }
                _logger.InfoFormat("Consumer removed from group: {0}, consumerId: {1}, connectionId: {2}, lastHeartbeat: {3}, subscriptionTopics: {4}, consumingQueues: {5}",
                    _groupName,
                    consumerInfo.ConsumerId,
                    connectionId,
                    consumerInfo.HeartbeatInfo.LastHeartbeatTime,
                    string.Join("|", consumerInfo.SubscriptionTopics),
                    string.Join("|", consumerInfo.ConsumingQueues));
            }
        }
        public void RemoveNotActiveConsumers()
        {
            foreach (var entry in _consumerInfoDict)
            {
                if (entry.Value.HeartbeatInfo.IsTimeout(BrokerController.Instance.Setting.ConsumerExpiredTimeout))
                {
                    RemoveConsumer(entry.Key);
                }
            }
        }
        public IEnumerable<string> GetAllConsumerIds()
        {
            return _consumerInfoDict.Values.Select(x => x.ConsumerId).ToList();
        }
        public int GetConsumerCount()
        {
            return _consumerInfoDict.Count;
        }
        public IEnumerable<string> GetConsumerIdsForTopic(string topic)
        {
            return _consumerInfoDict.Where(x => x.Value.SubscriptionTopics.Any(y => y == topic)).Select(z => z.Value.ConsumerId);
        }
        public IEnumerable<string> QueryConsumerIdsForTopic(string topic)
        {
            return _consumerInfoDict.Where(x => x.Value.SubscriptionTopics.Any(y => y.Contains(topic))).Select(z => z.Value.ConsumerId);
        }
        public bool IsConsumerExistForQueue(string topic, int queueId)
        {
            var key = new QueueKey(topic, queueId);
            return _consumerInfoDict.Values.Any(x => x.ConsumingQueues.Any(y => y == key));
        }
        public IEnumerable<QueueKey> GetConsumingQueue(string consumerId)
        {
            var consumerInfo = _consumerInfoDict.Values.SingleOrDefault(x => x.ConsumerId == consumerId);
            if (consumerInfo != null)
            {
                return consumerInfo.ConsumingQueues.ToList();
            }
            return new List<QueueKey>();
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
