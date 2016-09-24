using System.Collections.Generic;
using System.Linq;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;

namespace EQueue.Broker.Client
{
    public class GetConsumerListService
    {
        private readonly ConsumerManager _consumerManager;
        private readonly IQueueStore _queueStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;

        public GetConsumerListService(ConsumerManager consumerManager, IConsumeOffsetStore consumeOffsetStore, IQueueStore queueStore)
        {
            _consumerManager = consumerManager;
            _consumeOffsetStore = consumeOffsetStore;
            _queueStore = queueStore;
        }

        public IEnumerable<ConsumerInfo> GetAllConsumerList()
        {
            var consumerInfoList = new List<ConsumerInfo>();
            var consumerGroupList = _consumerManager.GetAllConsumerGroups();
            if (consumerGroupList == null || consumerGroupList.Count() == 0)
            {
                return consumerInfoList;
            }

            foreach (var consumerGroup in consumerGroupList)
            {
                var consumerIdList = consumerGroup.GetAllConsumerIds().ToList();
                consumerIdList.Sort();

                foreach (var consumerId in consumerIdList)
                {
                    var messageQueueList = consumerGroup.GetConsumingQueueList(consumerId);
                    foreach (var messageQueue in messageQueueList)
                    {
                        consumerInfoList.Add(BuildConsumerInfo(consumerGroup.GroupName, consumerId, messageQueue));
                    }
                }
            }

            consumerInfoList.Sort(SortConsumerInfo);

            return consumerInfoList;
        }
        public IEnumerable<ConsumerInfo> GetConsumerList(string groupName, string topic)
        {
            var consumerInfoList = new List<ConsumerInfo>();

            if (!string.IsNullOrEmpty(groupName) && !string.IsNullOrEmpty(topic))
            {
                var consumerGroup = _consumerManager.GetConsumerGroup(groupName);
                if (consumerGroup == null)
                {
                    return consumerInfoList;
                }

                var consumerIdList = consumerGroup.GetConsumerIdsForTopic(topic).ToList();
                consumerIdList.Sort();

                foreach (var consumerId in consumerIdList)
                {
                    var messageQueueList = consumerGroup.GetConsumingQueueList(consumerId).Where(x => x.Topic == topic);
                    foreach (var messageQueue in messageQueueList)
                    {
                        consumerInfoList.Add(BuildConsumerInfo(consumerGroup.GroupName, consumerId, messageQueue));
                    }
                }

                consumerInfoList.Sort(SortConsumerInfo);
            }

            return consumerInfoList;
        }

        private int SortConsumerInfo(ConsumerInfo x, ConsumerInfo y)
        {
            var result = string.Compare(x.ConsumerGroup, y.ConsumerGroup);
            if (result != 0)
            {
                return result;
            }
            result = string.Compare(x.ConsumerId, y.ConsumerId);
            if (result != 0)
            {
                return result;
            }
            result = string.Compare(x.Topic, y.Topic);
            if (result != 0)
            {
                return result;
            }
            if (x.QueueId > y.QueueId)
            {
                return 1;
            }
            else if (x.QueueId < y.QueueId)
            {
                return -1;
            }
            return 0;
        }
        private ConsumerInfo BuildConsumerInfo(string groupName, string consumerId, MessageQueueEx messageQueue)
        {
            var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(messageQueue.Topic, messageQueue.QueueId);
            var consumerInfo = new ConsumerInfo();
            consumerInfo.ConsumerGroup = groupName;
            consumerInfo.ConsumerId = consumerId;
            consumerInfo.Topic = messageQueue.Topic;
            consumerInfo.QueueId = messageQueue.QueueId;
            consumerInfo.ClientCachedMessageCount = messageQueue.ClientCachedMessageCount;
            consumerInfo.QueueCurrentOffset = queueCurrentOffset;
            consumerInfo.ConsumedOffset = _consumeOffsetStore.GetConsumeOffset(messageQueue.Topic, messageQueue.QueueId, groupName);
            consumerInfo.QueueNotConsumeCount = consumerInfo.CalculateQueueNotConsumeCount();
            return consumerInfo;
        }
    }
}
