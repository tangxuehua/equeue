using System.Collections.Concurrent;
using System.Collections.Generic;
using ECommon.IoC;
using ECommon.Logging;
using EQueue.Protocols;

namespace EQueue.Broker.Client
{
    public class ConsumerManager
    {
        private ILogger _logger;
        private ConcurrentDictionary<string, ConsumerGroup> _consumerGroupDict = new ConcurrentDictionary<string, ConsumerGroup>();

        public ConsumerManager()
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public void RegisterConsumer(string groupName, ClientChannel clientChannel, IEnumerable<string> subscriptionTopics)
        {
            var consumerGroup = _consumerGroupDict.GetOrAdd(groupName, new ConsumerGroup(groupName));
            consumerGroup.UpdateChannel(clientChannel);
            consumerGroup.UpdateSubscriptionTopics(subscriptionTopics);
        }
        public void ScanNotActiveConsumer()
        {
            foreach (var consumerGroup in _consumerGroupDict.Values)
            {
                consumerGroup.RemoteNotActiveConsumerChannels();
            }
        }
        public void RemoveConsumer(string consumerChannelRemotingAddress)
        {
            foreach (var consumerGroup in _consumerGroupDict.Values)
            {
                consumerGroup.RemoveConsumerChannel(consumerChannelRemotingAddress);
            }
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
    }
}
