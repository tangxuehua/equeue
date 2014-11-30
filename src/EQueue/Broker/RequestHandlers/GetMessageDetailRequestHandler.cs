using System.Collections.Generic;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class GetMessageDetailRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;

        public GetMessageDetailRequestHandler()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var getMessageDetailRequest = _binarySerializer.Deserialize<GetMessageDetailRequest>(request.Body);
            var message = _messageService.GetMessageDetail(getMessageDetailRequest.MessageOffset);
            var messages = new List<QueueMessage>();
            if (message != null)
            {
                messages.Add(message);
            }
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, _binarySerializer.Serialize(messages));
        }
    }
}
