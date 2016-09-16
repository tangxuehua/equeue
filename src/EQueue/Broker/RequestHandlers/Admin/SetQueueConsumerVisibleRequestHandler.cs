using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class SetQueueConsumerVisibleRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;

        public SetQueueConsumerVisibleRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            var request = _binarySerializer.Deserialize<SetQueueConsumerVisibleRequest>(remotingRequest.Body);
            _queueStore.SetConsumerVisible(request.Topic, request.QueueId, request.Visible);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
