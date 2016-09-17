using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.Broker.Client
{
    public class GetTopicConsumeInfoListService
    {
        private readonly ConsumerManager _consumerManager;
        private readonly IQueueStore _queueStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;

        public GetTopicConsumeInfoListService(ConsumerManager consumerManager, IConsumeOffsetStore consumeOffsetStore, IQueueStore queueStore)
        {
            _consumerManager = consumerManager;
            _consumeOffsetStore = consumeOffsetStore;
            _queueStore = queueStore;
        }

        public IEnumerable<TopicConsumeInfo> GetAllTopicConsumeInfoList()
        {
            var topicConsumeInfoList = _consumeOffsetStore.GetAllTopicConsumeInfoList();

            foreach (var topicConsumeInfo in topicConsumeInfoList)
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topicConsumeInfo.Topic, topicConsumeInfo.QueueId);
                topicConsumeInfo.QueueCurrentOffset = queueCurrentOffset;
                topicConsumeInfo.QueueNotConsumeCount = topicConsumeInfo.CalculateQueueNotConsumeCount();
                topicConsumeInfo.OnlineConsumerCount = _consumerManager.GetConsumerCount(topicConsumeInfo.ConsumerGroup);
            }

            return topicConsumeInfoList;
        }
        public IEnumerable<TopicConsumeInfo> GetTopicConsumeInfoList(string groupName, string topic)
        {
            var topicConsumeInfoList = _consumeOffsetStore.GetTopicConsumeInfoList(groupName, topic);

            foreach (var topicConsumeInfo in topicConsumeInfoList)
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topicConsumeInfo.Topic, topicConsumeInfo.QueueId);
                topicConsumeInfo.QueueCurrentOffset = queueCurrentOffset;
                topicConsumeInfo.QueueNotConsumeCount = topicConsumeInfo.CalculateQueueNotConsumeCount();
                topicConsumeInfo.OnlineConsumerCount = _consumerManager.GetConsumerCount(topicConsumeInfo.ConsumerGroup);
            }

            return topicConsumeInfoList;
        }
    }
}
