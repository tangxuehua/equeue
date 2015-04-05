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
        private BrokerController _brokerController;
        private ILogger _logger;

        public SendMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = MessageUtils.DecodeSendMessageRequest(remotingRequest.Body);
            var storeResult = _messageService.StoreMessage(request.Message, request.QueueId, request.RoutingKey);
            _brokerController.SuspendedPullRequestManager.NotifyNewMessage(request.Message.Topic, storeResult.QueueId, storeResult.QueueOffset);
            var responseData = Encoding.UTF8.GetBytes(string.Format("{0}:{1}:{2}", storeResult.MessageOffset, storeResult.QueueOffset, storeResult.MessageId));
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, responseData);
        }
    }
}
