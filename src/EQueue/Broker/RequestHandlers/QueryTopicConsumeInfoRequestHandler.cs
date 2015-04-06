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
        private ConsumerManager _consumerManager;
        private IBinarySerializer _binarySerializer;
        private IOffsetManager _offsetManager;
        private IQueueService _queueService;

        public QueryTopicConsumeInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
            _queueService = ObjectContainer.Resolve<IQueueService>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<QueryTopicConsumeInfoRequest>(remotingRequest.Body);
            var topicConsumeInfoList = _offsetManager.QueryTopicConsumeInfos(request.GroupName, request.Topic).ToList().Where(x => _queueService.IsQueueExist(x.Topic, x.QueueId)).ToList();

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
                var consumerGroup = _consumerManager.GetConsumerGroup(topicConsumeInfo.ConsumerGroup);
                topicConsumeInfo.HasConsumer = consumerGroup != null && consumerGroup.GetAllConsumerIds().Count() > 0;
                var queueCurrentOffset = _queueService.GetQueueCurrentOffset(topicConsumeInfo.Topic, topicConsumeInfo.QueueId);
                topicConsumeInfo.QueueMaxOffset = queueCurrentOffset;
                topicConsumeInfo.UnConsumedMessageCount = topicConsumeInfo.QueueMaxOffset - topicConsumeInfo.ConsumedOffset;
            }

            var data = _binarySerializer.Serialize(topicConsumeInfoList);
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, data);
        }
    }
}
