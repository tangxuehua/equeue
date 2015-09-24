using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Broker.Client
{
    public class ConsumerManager
    {
        private readonly ConcurrentDictionary<string, ConsumerGroup> _consumerGroupDict = new ConcurrentDictionary<string, ConsumerGroup>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        public ConsumerManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _consumerGroupDict.Clear();
            _scheduleService.StartTask("ConsumerManager.ScanNotActiveConsumer", ScanNotActiveConsumer, 1000 * 5, 1000 * 5);
        }
        public void Shutdown()
        {
            _consumerGroupDict.Clear();
            _scheduleService.StopTask("ConsumerManager.ScanNotActiveConsumer");
        }
        public void RegisterConsumer(string groupName, ClientChannel clientChannel, IEnumerable<string> subscriptionTopics, IEnumerable<string> consumingQueues)
        {
            var consumerGroup = _consumerGroupDict.GetOrAdd(groupName, new ConsumerGroup(groupName, this));
            consumerGroup.Register(clientChannel);
            consumerGroup.UpdateConsumerSubscriptionTopics(clientChannel, subscriptionTopics);
            consumerGroup.UpdateConsumerConsumingQueues(clientChannel, consumingQueues);
        }
        public void RemoveConsumer(string consumerRemotingAddress)
        {
            foreach (var consumerGroup in _consumerGroupDict.Values)
            {
                consumerGroup.RemoveConsumer(consumerRemotingAddress);
            }
        }
        public bool IsConsumerExistForQueue(string topic, int queueId)
        {
            return _consumerGroupDict.Values.Any(x => x.IsConsumerExistForQueue(topic, queueId));
        }
        public IEnumerable<ConsumerGroup> GetAllConsumerGroups()
        {
            return _consumerGroupDict.Values.ToList();
        }
        public ConsumerGroup GetConsumerGroup(string groupName)
        {
            ConsumerGroup consumerGroup;
            if (_consumerGroupDict.TryGetValue(groupName, out consumerGroup))
            {
                return consumerGroup;
            }
            return consumerGroup;
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
