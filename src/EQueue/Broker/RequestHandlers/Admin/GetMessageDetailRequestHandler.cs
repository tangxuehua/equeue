using System.Collections.Generic;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Protocols;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
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
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            var messages = new List<QueueMessage>();
            var request = _binarySerializer.Deserialize<GetMessageDetailRequest>(remotingRequest.Body);
            var messageInfo = MessageIdUtil.ParseMessageId(request.MessageId);
            var currentBrokerName = BrokerController.Instance.Setting.BrokerInfo.BrokerName;
            if (currentBrokerName != messageInfo.BrokerName)
            {
                return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(messages));
            }

            var message = _messageStore.GetMessage(messageInfo.MessagePosition);
            if (message != null)
            {
                messages.Add(message);
            }
            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(messages));
        }
    }
}
