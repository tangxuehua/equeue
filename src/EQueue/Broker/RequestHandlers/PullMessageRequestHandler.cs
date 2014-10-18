using System;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.LongPolling;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class PullMessageRequestHandler : IRequestHandler
    {
        private BrokerController _brokerController;
        private IMessageService _messageService;
        private IOffsetManager _offsetManager;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public PullMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var pullMessageRequest = _binarySerializer.Deserialize<PullMessageRequest>(request.Body);
            if (pullMessageRequest.QueueOffset < 0)
            {
                var lastConsumedQueueOffset = _offsetManager.GetQueueOffset(
                    pullMessageRequest.MessageQueue.Topic,
                    pullMessageRequest.MessageQueue.QueueId,
                    pullMessageRequest.ConsumerGroup);
                var queueCurrentOffset = _messageService.GetQueueCurrentOffset(
                    pullMessageRequest.MessageQueue.Topic,
                    pullMessageRequest.MessageQueue.QueueId);

                var nextQueueOffset = lastConsumedQueueOffset + 1;
                if (lastConsumedQueueOffset == -1)
                {
                    nextQueueOffset = queueCurrentOffset + 1;
                }

                var response = new PullMessageResponse(new QueueMessage[0], nextQueueOffset);
                var responseData = _binarySerializer.Serialize(response);
                return new RemotingResponse((int)PullStatus.NextOffsetReset, request.Sequence, responseData);
            }
            var messages = _messageService.GetMessages(
                pullMessageRequest.MessageQueue.Topic,
                pullMessageRequest.MessageQueue.QueueId,
                pullMessageRequest.QueueOffset,
                pullMessageRequest.PullMessageBatchSize);
            if (messages.Count() > 0)
            {
                _logger.InfoFormat("Pulled messages, topic:{0}, queueId:{1}, msgCount:{2}",
                    pullMessageRequest.MessageQueue.Topic,
                    pullMessageRequest.MessageQueue.QueueId,
                    messages.Count());
                var pullMessageResponse = new PullMessageResponse(messages);
                var responseData = _binarySerializer.Serialize(pullMessageResponse);
                return new RemotingResponse((int)PullStatus.Found, request.Sequence, responseData);
            }
            else
            {
                if (pullMessageRequest.SuspendPullRequestMilliseconds > 0)
                {
                    var pullRequest = new PullRequest(
                        request.Sequence,
                        pullMessageRequest,
                        context,
                        DateTime.Now,
                        pullMessageRequest.SuspendPullRequestMilliseconds,
                        ExecutePullRequest,
                        ExecutePullRequest,
                        ExecuteReplacedPullRequest);
                    _brokerController.SuspendedPullRequestManager.SuspendPullRequest(pullRequest);
                    return null;
                }
                else
                {
                    var queueMinOffset = _messageService.GetQueueMinOffset(pullMessageRequest.MessageQueue.Topic, pullMessageRequest.MessageQueue.QueueId);
                    if (queueMinOffset > pullMessageRequest.QueueOffset)
                    {
                        var response = new PullMessageResponse(new QueueMessage[0], queueMinOffset);
                        var responseData = _binarySerializer.Serialize(response);
                        return new RemotingResponse((int)PullStatus.NextOffsetReset, request.Sequence, responseData);
                    }
                    else
                    {
                        var response = new PullMessageResponse(messages);
                        var responseData = _binarySerializer.Serialize(response);
                        return new RemotingResponse((int)PullStatus.NoNewMessage, request.Sequence, responseData);
                    }
                }
            }
        }

        private void ExecutePullRequest(PullRequest pullRequest)
        {
            var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(pullRequest.PullMessageRequest.ConsumerGroup);
            if (consumerGroup != null && consumerGroup.IsConsumerActive(pullRequest.RequestHandlerContext.Channel.RemoteEndPoint.ToString()))
            {
                var pullMessageRequest = pullRequest.PullMessageRequest;
                var messages = _messageService.GetMessages(
                    pullMessageRequest.MessageQueue.Topic,
                    pullMessageRequest.MessageQueue.QueueId,
                    pullMessageRequest.QueueOffset,
                    pullMessageRequest.PullMessageBatchSize);

                if (messages.Count() > 0)
                {
                    var response = new PullMessageResponse(messages);
                    var responseData = _binarySerializer.Serialize(response);
                    var remotingResponse = new RemotingResponse((int)PullStatus.Found, pullRequest.RemotingRequestSequence, responseData);
                    pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
                    return;
                }

                var queueMinOffset = _messageService.GetQueueMinOffset(pullMessageRequest.MessageQueue.Topic, pullMessageRequest.MessageQueue.QueueId);
                if (queueMinOffset > pullMessageRequest.QueueOffset)
                {
                    var response = new PullMessageResponse(new QueueMessage[0], queueMinOffset);
                    var responseData = _binarySerializer.Serialize(response);
                    var remotingResponse = new RemotingResponse((int)PullStatus.NextOffsetReset, pullRequest.RemotingRequestSequence, responseData);
                    pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
                }
                else
                {
                    var response = new PullMessageResponse(new QueueMessage[0]);
                    var responseData = _binarySerializer.Serialize(response);
                    var remotingResponse = new RemotingResponse((int)PullStatus.NoNewMessage, pullRequest.RemotingRequestSequence, responseData);
                    pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
                }
            }
        }
        private void ExecuteReplacedPullRequest(PullRequest pullRequest)
        {
            var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(pullRequest.PullMessageRequest.ConsumerGroup);
            if (consumerGroup != null && consumerGroup.IsConsumerActive(pullRequest.RequestHandlerContext.Channel.RemoteEndPoint.ToString()))
            {
                var responseData = _binarySerializer.Serialize(new PullMessageResponse(new QueueMessage[0]));
                var remotingResponse = new RemotingResponse((int)PullStatus.Ignored, pullRequest.RemotingRequestSequence, responseData);
                pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
            }
        }
    }
}
