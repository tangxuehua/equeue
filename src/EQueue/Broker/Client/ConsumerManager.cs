using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;

namespace EQueue.Broker.Client
{
    public class ConsumerManager
    {
        private ILogger _logger;
        private ConcurrentDictionary<string, ConsumerGroupInfo> _consumerGroupDict = new ConcurrentDictionary<string, ConsumerGroupInfo>();

        public ConsumerManager()
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public bool RegisterConsumer(string group, ClientChannelInfo clientChannelInfo, MessageModel messageModel, IEnumerable<string> subscriptionTopics)
        {
            var consumerGroupInfo = _consumerGroupDict.GetOrAdd(group, new ConsumerGroupInfo(group, messageModel));
            var channelChanged = consumerGroupInfo.UpdateChannel(clientChannelInfo, messageModel);
            var subscriptionTopicChanged = consumerGroupInfo.UpdateSubscriptionTopics(subscriptionTopics);
            var changed = channelChanged || subscriptionTopicChanged;

            if (changed)
            {
                //TODO, notify consumer group changed.
            }

            return changed;
        }
        public void ScanNotActiveChannel()
        {
            foreach (var consumerGroup in _consumerGroupDict.Values)
            {
                consumerGroup.RemoteNotActiveChannels();
            }
        }
    }
}
