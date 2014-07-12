using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryTopicConsumeInfoRequestHandler : IRequestHandler
    {
        private BrokerController _brokerController;
        private IBinarySerializer _binarySerializer;
        private IOffsetManager _offsetManager;

        public QueryTopicConsumeInfoRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var queryTopicConsumeInfoRequest = _binarySerializer.Deserialize<QueryTopicConsumeInfoRequest>(request.Body);
            var topicConsumeInfoList = new List<TopicConsumeInfo>();

            if (!string.IsNullOrEmpty(queryTopicConsumeInfoRequest.GroupName))
            {
                var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(queryTopicConsumeInfoRequest.GroupName);
                if (consumerGroup != null)
                {
                    foreach (var topicConsumeInfo in GetTopicConsumeInfoForGroup(consumerGroup, queryTopicConsumeInfoRequest.Topic))
                    {
                        topicConsumeInfoList.Add(topicConsumeInfo);
                    }
                }
            }
            else
            {
                var consumerGroups = _brokerController.ConsumerManager.GetAllConsumerGroups();
                foreach (var consumerGroup in consumerGroups)
                {
                    foreach (var topicConsumeInfo in GetTopicConsumeInfoForGroup(consumerGroup, queryTopicConsumeInfoRequest.Topic))
                    {
                        topicConsumeInfoList.Add(topicConsumeInfo);
                    }
                }
            }

            var data = _binarySerializer.Serialize(topicConsumeInfoList);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, data);
        }

        private IEnumerable<TopicConsumeInfo> GetTopicConsumeInfoForGroup(ConsumerGroup consumerGroup, string currentTopic)
        {
            var topicConsumeInfoList = new List<TopicConsumeInfo>();
            var consumerIdList = string.IsNullOrEmpty(currentTopic) ? consumerGroup.GetAllConsumerIds().ToList() : consumerGroup.GetConsumerIdsForTopic(currentTopic).ToList();
            consumerIdList.Sort();

            foreach (var consumerId in consumerIdList)
            {
                var consumingQueues = consumerGroup.GetConsumingQueue(consumerId);
                foreach (var consumingQueue in consumingQueues)
                {
                    var items = consumingQueue.Split('-');
                    var topic = items[0];
                    var queueId = int.Parse(items[1]);
                    if (string.IsNullOrEmpty(currentTopic) || topic == currentTopic)
                    {
                        topicConsumeInfoList.Add(BuildTopicConsumeInfo(consumerGroup.GroupName, consumerId, topic, queueId));
                    }
                }
            }

            return topicConsumeInfoList;
        }
        private TopicConsumeInfo BuildTopicConsumeInfo(string group, string consumerId, string topic, int queueId)
        {
            var topicConsumeInfo = new TopicConsumeInfo();
            topicConsumeInfo.ConsumerGroup = group;
            topicConsumeInfo.ConsumerId = consumerId;
            topicConsumeInfo.Topic = topic;
            topicConsumeInfo.QueueId = queueId;
            topicConsumeInfo.ConsumedOffset = _offsetManager.GetQueueOffset(topic, queueId, group);
            return topicConsumeInfo;
        }
    }
}
