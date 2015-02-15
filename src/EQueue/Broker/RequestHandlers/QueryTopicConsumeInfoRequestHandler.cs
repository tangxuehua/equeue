using System.Linq;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryTopicConsumeInfoRequestHandler : IRequestHandler
    {
        private BrokerController _brokerController;
        private IBinarySerializer _binarySerializer;
        private IOffsetManager _offsetManager;
        private IMessageService _messageService;

        public QueryTopicConsumeInfoRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var queryTopicConsumeInfoRequest = _binarySerializer.Deserialize<QueryTopicConsumeInfoRequest>(request.Body);
            var topicConsumeInfoList = _offsetManager.QueryTopicConsumeInfos(queryTopicConsumeInfoRequest.GroupName, queryTopicConsumeInfoRequest.Topic).ToList();

            topicConsumeInfoList.Sort((x, y) =>
            {
                var result = string.Compare(x.ConsumerGroup, y.ConsumerGroup);
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

            foreach (var topicConsumeInfo in topicConsumeInfoList)
            {
                var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(topicConsumeInfo.ConsumerGroup);
                topicConsumeInfo.HasConsumer = consumerGroup != null && consumerGroup.GetAllConsumerIds().Count() > 0;
                var queueCurrentOffset = _messageService.GetQueueCurrentOffset(topicConsumeInfo.Topic, topicConsumeInfo.QueueId);
                topicConsumeInfo.QueueMaxOffset = queueCurrentOffset;
                topicConsumeInfo.UnConsumedMessageCount = topicConsumeInfo.QueueMaxOffset - topicConsumeInfo.ConsumedOffset;
            }

            var data = _binarySerializer.Serialize(topicConsumeInfoList);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, data);
        }
    }
}
