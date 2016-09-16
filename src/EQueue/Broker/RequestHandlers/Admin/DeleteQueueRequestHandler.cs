using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class DeleteQueueRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;

        public DeleteQueueRequestHandler()
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
            var request = _binarySerializer.Deserialize<DeleteQueueRequest>(remotingRequest.Body);
            _queueStore.DeleteQueue(request.Topic, request.QueueId);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
