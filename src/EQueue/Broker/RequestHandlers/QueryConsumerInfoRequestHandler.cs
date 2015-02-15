using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryConsumerInfoRequestHandler : IRequestHandler
    {
        private BrokerController _brokerController;
        private IBinarySerializer _binarySerializer;
        private IOffsetManager _offsetManager;
        private IMessageService _messageService;

        public QueryConsumerInfoRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var queryConsumerInfoRequest = _binarySerializer.Deserialize<QueryConsumerInfoRequest>(request.Body);
            var consumerInfoList = new List<ConsumerInfo>();

            if (!string.IsNullOrEmpty(queryConsumerInfoRequest.GroupName))
            {
                var consumerGroups = _brokerController.ConsumerManager.QueryConsumerGroup(queryConsumerInfoRequest.GroupName);
                foreach (var consumerGroup in consumerGroups)
                {
                    foreach (var topicConsumeInfo in GetConsumerInfoForGroup(consumerGroup, queryConsumerInfoRequest.Topic))
                    {
                        consumerInfoList.Add(topicConsumeInfo);
                    }
                }
            }
            else
            {
                var consumerGroups = _brokerController.ConsumerManager.GetAllConsumerGroups();
                foreach (var consumerGroup in consumerGroups)
                {
                    foreach (var topicConsumeInfo in GetConsumerInfoForGroup(consumerGroup, queryConsumerInfoRequest.Topic))
                    {
                        consumerInfoList.Add(topicConsumeInfo);
                    }
                }
            }

            var data = _binarySerializer.Serialize(consumerInfoList);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, data);
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
            var queueCurrentOffset = _messageService.GetQueueCurrentOffset(topic, queueId);
            var consumerInfo = new ConsumerInfo();
            consumerInfo.ConsumerGroup = group;
            consumerInfo.ConsumerId = consumerId;
            consumerInfo.Topic = topic;
            consumerInfo.QueueId = queueId;
            consumerInfo.QueueMaxOffset = queueCurrentOffset;
            consumerInfo.ConsumedOffset = _offsetManager.GetQueueOffset(topic, queueId, group);
            consumerInfo.UnConsumedMessageCount = consumerInfo.QueueMaxOffset - consumerInfo.ConsumedOffset;
            return consumerInfo;
        }
    }
}
