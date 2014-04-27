using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.Processors
{
    public class SendMessageRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;
        private BrokerController _brokerController;
        private ILogger _logger;

        public SendMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var sendMessageRequest = MessageUtils.DecodeSendMessageRequest(request.Body);
            var storeResult = _messageService.StoreMessage(sendMessageRequest.Message, sendMessageRequest.QueueId);
            if (_brokerController.Setting.NotifyWhenMessageArrived)
            {
                _brokerController.SuspendedPullRequestManager.NotifyMessageArrived(sendMessageRequest.Message.Topic, storeResult.QueueId, storeResult.QueueOffset);
            }
            var sendMessageResponse = new SendMessageResponse(
                storeResult.MessageOffset,
                new MessageQueue(sendMessageRequest.Message.Topic, storeResult.QueueId),
                storeResult.QueueOffset);
            var responseData = _binarySerializer.Serialize(sendMessageResponse);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, responseData);
        }
    }
}
