using System.Text;
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
            var storeResult = _messageService.StoreMessage(sendMessageRequest.Message, sendMessageRequest.QueueId, sendMessageRequest.RoutingKey);
            _brokerController.SuspendedPullRequestManager.NotifyNewMessage(sendMessageRequest.Message.Topic, storeResult.QueueId, storeResult.QueueOffset);
            var responseData = Encoding.UTF8.GetBytes(string.Format("{0}:{1}", storeResult.MessageOffset, storeResult.QueueOffset));
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, responseData);
        }
    }
}
