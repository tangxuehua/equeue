using System;
using System.Linq;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.LongPolling;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class PullMessageRequestHandler : IRequestHandler
    {
        private const int SuspendPullRequestMilliseconds = 60 * 1000;
        private BrokerController _brokerController;
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public PullMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var pullMessageRequest = _binarySerializer.Deserialize<PullMessageRequest>(request.Body);
            var messages = _messageService.GetMessages(
                pullMessageRequest.MessageQueue.Topic,
                pullMessageRequest.MessageQueue.QueueId,
                pullMessageRequest.QueueOffset,
                pullMessageRequest.PullMessageBatchSize);
            if (messages.Count() > 0)
            {
                var pullMessageResponse = new PullMessageResponse(messages);
                var responseData = _binarySerializer.Serialize(pullMessageResponse);
                return new RemotingResponse((int)PullStatus.Found, request.Sequence, responseData);
            }
            else
            {
                var pullRequest = new PullRequest(
                    request.Sequence,
                    pullMessageRequest,
                    context,
                    DateTime.Now,
                    SuspendPullRequestMilliseconds,
                    ExecutePullRequest,
                    ExecutePullRequest,
                    ExecuteReplacedPullRequest);
                _brokerController.SuspendedPullRequestManager.SuspendPullRequest(pullRequest);
                return null;
            }
        }

        private void ExecutePullRequest(PullRequest pullRequest)
        {
            var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(pullRequest.PullMessageRequest.ConsumerGroup);
            if (consumerGroup != null && consumerGroup.IsConsumerChannelActive(pullRequest.RequestHandlerContext.Channel.RemotingAddress))
            {
                var pullMessageRequest = pullRequest.PullMessageRequest;
                var messages = _messageService.GetMessages(
                    pullMessageRequest.MessageQueue.Topic,
                    pullMessageRequest.MessageQueue.QueueId,
                    pullMessageRequest.QueueOffset,
                    pullMessageRequest.PullMessageBatchSize);
                var pullMessageResponse = new PullMessageResponse(messages);
                var responseData = _binarySerializer.Serialize(pullMessageResponse);
                var remotingResponse = new RemotingResponse(messages.Count() > 0 ? (int)PullStatus.Found : (int)PullStatus.NoNewMessage, pullRequest.RemotingRequestSequence, responseData);
                pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
            }
        }
        private void ExecuteReplacedPullRequest(PullRequest pullRequest)
        {
            var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(pullRequest.PullMessageRequest.ConsumerGroup);
            if (consumerGroup != null && consumerGroup.IsConsumerChannelActive(pullRequest.RequestHandlerContext.Channel.RemotingAddress))
            {
                var responseData = _binarySerializer.Serialize(new PullMessageResponse(new QueueMessage[0]));
                var remotingResponse = new RemotingResponse((int)PullStatus.Ignored, pullRequest.RemotingRequestSequence, responseData);
                pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
            }
        }
    }
}
