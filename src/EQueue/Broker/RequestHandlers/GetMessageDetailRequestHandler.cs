using System.Collections.Generic;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class GetMessageDetailRequestHandler : IRequestHandler
    {
        private readonly IMessageStore _messageStore;
        private readonly IBinarySerializer _binarySerializer;

        public GetMessageDetailRequestHandler()
        {
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetMessageDetailRequest>(remotingRequest.Body);
            var message = _messageStore.FindMessage(request.MessageOffset, request.MessageId);
            var messages = new List<QueueMessage>();
            if (message != null)
            {
                messages.Add(message);
            }
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, _binarySerializer.Serialize(messages));
        }
    }
}
