using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Scheduling;
using ECommon.Socketing;

namespace EQueue.Broker.Client
{
    public class ConsumerManager
    {
        private readonly ConcurrentDictionary<string, ConsumerGroup> _consumerGroupDict = new ConcurrentDictionary<string, ConsumerGroup>();
        private readonly IScheduleService _scheduleService;

        public ConsumerManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
        }

        public void Start()
        {
            _consumerGroupDict.Clear();
            _scheduleService.StartTask("ScanNotActiveConsumer", ScanNotActiveConsumer, 1000, 1000);
        }
        public void Shutdown()
        {
            _consumerGroupDict.Clear();
            _scheduleService.StopTask("ScanNotActiveConsumer");
        }
        public void RegisterConsumer(string groupName, string consumerId, IEnumerable<string> subscriptionTopics, IEnumerable<string> consumingQueues, ITcpConnection connection)
        {
            var consumerGroup = _consumerGroupDict.GetOrAdd(groupName, key => new ConsumerGroup(key));
            consumerGroup.Register(consumerId, connection);
            consumerGroup.UpdateConsumerSubscriptionTopics(consumerId, subscriptionTopics);
            consumerGroup.UpdateConsumerConsumingQueues(consumerId, consumingQueues);
        }
        public void RemoveConsumer(string consumerId)
        {
            foreach (var consumerGroup in _consumerGroupDict.Values)
            {
                consumerGroup.RemoveConsumer(consumerId);
            }
        }
        public int GetConsumerGroupCount()
        {
            return _consumerGroupDict.Count;
        }
        public IEnumerable<ConsumerGroup> GetAllConsumerGroups()
        {
            return _consumerGroupDict.Values.ToList();
        }
        public int GetConsumerCount()
        {
            return GetAllConsumerGroups().Sum(x => x.GetConsumerCount());
        }
        public ConsumerGroup GetConsumerGroup(string groupName)
        {
            ConsumerGroup consumerGroup;
            if (_consumerGroupDict.TryGetValue(groupName, out consumerGroup))
            {
                return consumerGroup;
            }
            return null;
        }
        public bool IsConsumerActive(string consumerGroup, string consumerId)
        {
            var group = GetConsumerGroup(consumerGroup);
            return group != null && group.IsConsumerActive(consumerId);
        }
        public bool IsConsumerExistForQueue(string topic, int queueId)
        {
            var groups = GetAllConsumerGroups();
            foreach (var group in groups)
            {
                if (group.IsConsumerExistForQueue(topic, queueId))
                {
                    return true;
                }
            }
            return false;
        }
        public IEnumerable<ConsumerGroup> QueryConsumerGroup(string groupName)
        {
            return _consumerGroupDict.Where(x => x.Key.Contains(groupName)).Select(x => x.Value);
        }

        private void ScanNotActiveConsumer()
        {
            foreach (var consumerGroup in _consumerGroupDict.Values)
            {
                consumerGroup.RemoveNotActiveConsumers();
            }
        }
    }
}
