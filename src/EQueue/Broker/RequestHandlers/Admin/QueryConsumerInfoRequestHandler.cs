using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.Exceptions;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class QueryConsumerInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IConsumeOffsetStore _offsetStore;
        private ConsumerManager _consumerManager;
        private IQueueStore _queueStore;

        public QueryConsumerInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = _binarySerializer.Deserialize<QueryConsumerInfoRequest>(remotingRequest.Body);
            var consumerInfoList = new List<ConsumerInfo>();

            if (!string.IsNullOrEmpty(request.GroupName))
            {
                var consumerGroups = _consumerManager.QueryConsumerGroup(request.GroupName);
                foreach (var consumerGroup in consumerGroups)
                {
                    foreach (var topicConsumeInfo in GetConsumerInfoForGroup(consumerGroup, request.Topic))
                    {
                        consumerInfoList.Add(topicConsumeInfo);
                    }
                }
            }
            else
            {
                var consumerGroups = _consumerManager.GetAllConsumerGroups();
                foreach (var consumerGroup in consumerGroups)
                {
                    foreach (var topicConsumeInfo in GetConsumerInfoForGroup(consumerGroup, request.Topic))
                    {
                        consumerInfoList.Add(topicConsumeInfo);
                    }
                }
            }

            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(consumerInfoList));
        }

        private IEnumerable<ConsumerInfo> GetConsumerInfoForGroup(ConsumerGroup consumerGroup, string currentTopic)
        {
            var consumerInfoList = new List<ConsumerInfo>();
            var consumerIdList = string.IsNullOrEmpty(currentTopic) ? consumerGroup.GetAllConsumerIds().ToList() : consumerGroup.QueryConsumerIdsForTopic(currentTopic).ToList();
            consumerIdList.Sort();

            foreach (var consumerId in consumerIdList)
            {
                var consumingQueues = consumerGroup.GetConsumingQueue(consumerId);
                foreach (var consumingQueue in consumingQueues)
                {
                    var items = consumingQueue.Split('-');
                    var topic = items[0];
                    var queueId = int.Parse(items[1]);
                    if (string.IsNullOrEmpty(currentTopic) || topic.Contains(currentTopic))
                    {
                        consumerInfoList.Add(BuildConsumerInfo(consumerGroup.GroupName, consumerId, topic, queueId));
                    }
                }
            }

            consumerInfoList.Sort((x, y) =>
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
            });

            return consumerInfoList;
        }
        private ConsumerInfo BuildConsumerInfo(string group, string consumerId, string topic, int queueId)
        {
            var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topic, queueId);
            var consumerInfo = new ConsumerInfo();
            consumerInfo.ConsumerGroup = group;
            consumerInfo.ConsumerId = consumerId;
            consumerInfo.Topic = topic;
            consumerInfo.QueueId = queueId;
            consumerInfo.QueueCurrentOffset = queueCurrentOffset;
            consumerInfo.ConsumedOffset = _offsetStore.GetConsumeOffset(topic, queueId, group);
            return consumerInfo;
        }
    }
}
