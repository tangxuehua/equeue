using System.Collections.Generic;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Protocols;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetMessageDetailByQueueOffsetRequestHandler : IRequestHandler
    {
        private readonly IQueueStore _queueStore;
        private readonly IMessageStore _messageStore;
        private readonly IBinarySerializer _binarySerializer;

        public GetMessageDetailByQueueOffsetRequestHandler()
        {
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            var request = _binarySerializer.Deserialize<GetMessageDetailByQueueOffsetRequest>(remotingRequest.Body);
            var queue = _queueStore.GetQueue(request.Topic, request.QueueId);
            var messages = new List<QueueMessage>();
            if (queue != null)
            {
                var messagePosition = queue.GetMessagePosition(request.QueueOffset, out int tagCode);
                var message = _messageStore.GetMessage(messagePosition);
                if (message != null)
                {
                    messages.Add(message);
                }
            }
            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(messages));
        }
    }
}
