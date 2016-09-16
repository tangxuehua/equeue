using System.Collections.Generic;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class CreateTopicRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;

        public CreateTopicRequestHandler()
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
            var request = _binarySerializer.Deserialize<CreateTopicRequest>(remotingRequest.Body);
            IEnumerable<int> queueIds = _queueStore.CreateTopic(request.Topic, request.InitialQueueCount);
            var data = _binarySerializer.Serialize(queueIds);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
