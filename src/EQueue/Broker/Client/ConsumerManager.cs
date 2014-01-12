using System.Collections.Concurrent;
using System.Collections.Generic;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
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

        public bool RegisterConsumer(string groupName, ClientChannel clientChannel, MessageModel messageModel, IEnumerable<string> subscriptionTopics)
        {
            var consumerGroup = _consumerGroupDict.GetOrAdd(groupName, new ConsumerGroup(groupName, messageModel));
            var channelChanged = consumerGroup.UpdateChannel(clientChannel, messageModel);
            var subscriptionTopicChanged = consumerGroup.UpdateSubscriptionTopics(subscriptionTopics);
            var changed = channelChanged || subscriptionTopicChanged;

            if (changed)
            {
                //TODO, notify consumer group changed.
            }

            return changed;
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
    }
}
