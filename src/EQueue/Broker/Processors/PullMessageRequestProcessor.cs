using System;
using System.Linq;
using EQueue.Broker.LongPolling;
using EQueue.Infrastructure;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;
using EQueue.Remoting.Requests;
using EQueue.Remoting.Responses;

namespace EQueue.Broker.Processors
{
    public class PullMessageRequestProcessor : IRequestProcessor
    {
        private const int SuspendPullRequestMilliseconds = 15 * 1000;
        private BrokerController _brokerController;
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public PullMessageRequestProcessor(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse ProcessRequest(IRequestHandlerContext context, RemotingRequest request)
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
                var remotingResponse = new RemotingResponse((int)PullStatus.Found, responseData);
                remotingResponse.Sequence = request.Sequence;
                return remotingResponse;
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
                    ExecutePullRequest);
                _brokerController.PullRequestHoldService.SuspendPullRequest(pullRequest);
                return null;
            }
        }

        private void ExecutePullRequest(PullRequest pullRequest)
        {
            var pullMessageRequest = pullRequest.PullMessageRequest;
            var messages = _messageService.GetMessages(
                pullMessageRequest.MessageQueue.Topic,
                pullMessageRequest.MessageQueue.QueueId,
                pullMessageRequest.QueueOffset,
                pullMessageRequest.PullMessageBatchSize);
            var pullMessageResponse = new PullMessageResponse(messages);
            var responseData = _binarySerializer.Serialize(pullMessageResponse);
            var remotingResponse = new RemotingResponse(messages.Count() > 0 ? (int)PullStatus.Found : (int)PullStatus.NoNewMessage, responseData);
            remotingResponse.Sequence = pullRequest.RemotingRequestSequence;
            pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
        }
    }
}
