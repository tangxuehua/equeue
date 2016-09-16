using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetTopicConsumeInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IConsumeOffsetStore _offsetStore;
        private IQueueStore _queueStore;
        private ConsumerManager _consumerManager;

        public GetTopicConsumeInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = _binarySerializer.Deserialize<GetTopicConsumeInfoRequest>(remotingRequest.Body);
            var topicConsumeInfoList = _offsetStore.GetTopicConsumeInfoList(request.GroupName, request.Topic);

            foreach (var topicConsumeInfo in topicConsumeInfoList)
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topicConsumeInfo.Topic, topicConsumeInfo.QueueId);
                topicConsumeInfo.QueueCurrentOffset = queueCurrentOffset;
                topicConsumeInfo.QueueNotConsumeCount = topicConsumeInfo.CalculateQueueNotConsumeCount();
                topicConsumeInfo.OnlineConsumerCount = _consumerManager.GetConsumerCount(request.GroupName);
            }

            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(topicConsumeInfoList));
        }
    }
}
