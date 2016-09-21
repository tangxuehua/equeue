using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols.Brokers.Requests;

namespace EQueue.Broker.RequestHandlers
{
    public class UpdateQueueConsumeOffsetRequestHandler : IRequestHandler
    {
        private IConsumeOffsetStore _offsetStore;
        private IBinarySerializer _binarySerializer;
        private readonly ITpsStatisticService _tpsStatisticService;

        public UpdateQueueConsumeOffsetRequestHandler()
        {
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _tpsStatisticService = ObjectContainer.Resolve<ITpsStatisticService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                return null;
            }

            var request = _binarySerializer.Deserialize<UpdateQueueOffsetRequest>(remotingRequest.Body);
            _offsetStore.UpdateConsumeOffset(
                request.MessageQueue.Topic,
                request.MessageQueue.QueueId,
                request.QueueOffset,
                request.ConsumerGroup);
            _tpsStatisticService.UpdateTopicConsumeOffset(
                request.MessageQueue.Topic,
                request.MessageQueue.QueueId,
                request.ConsumerGroup,
                request.QueueOffset);
            return null;
        }
    }
}
