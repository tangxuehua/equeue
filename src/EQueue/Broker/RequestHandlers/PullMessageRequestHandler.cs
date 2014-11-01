using System;
using System.Collections.Generic;
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
        private byte[] _emptyResponseData;

        public PullMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _emptyResponseData = _binarySerializer.Serialize(new PullMessageResponse(new QueueMessage[0]));
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var pullMessageRequest = _binarySerializer.Deserialize<PullMessageRequest>(request.Body);

            if (pullMessageRequest.QueueOffset < 0)
            {
                var lastConsumedQueueOffset = _offsetManager.GetQueueOffset(pullMessageRequest.MessageQueue.Topic, pullMessageRequest.MessageQueue.QueueId, pullMessageRequest.ConsumerGroup);
                var queueCurrentOffset = _messageService.GetQueueCurrentOffset(pullMessageRequest.MessageQueue.Topic, pullMessageRequest.MessageQueue.QueueId);
                var nextQueueOffset = lastConsumedQueueOffset + 1;

                if (lastConsumedQueueOffset == -1)
                {
                    nextQueueOffset = queueCurrentOffset + 1;
                }

                return BuildNextOffsetResetResponse(request.Sequence, nextQueueOffset);
            }

            var messages = _messageService.GetMessages(
                pullMessageRequest.MessageQueue.Topic,
                pullMessageRequest.MessageQueue.QueueId,
                pullMessageRequest.QueueOffset,
                pullMessageRequest.PullMessageBatchSize);

            if (messages.Count() > 0)
            {
                return BuildFoundResponse(request.Sequence, messages);
            }

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

            var queueMinOffset = _messageService.GetQueueMinOffset(pullMessageRequest.MessageQueue.Topic, pullMessageRequest.MessageQueue.QueueId);
            if (queueMinOffset > pullMessageRequest.QueueOffset)
            {
                return BuildNextOffsetResetResponse(request.Sequence, queueMinOffset);
            }
            else
            {
                return BuildNoNewMessageResponse(request.Sequence);
            }
        }

        private void ExecutePullRequest(PullRequest pullRequest)
        {
            if (!IsPullRequestValid(pullRequest))
            {
                return;
            }

            var pullMessageRequest = pullRequest.PullMessageRequest;
            var messages = _messageService.GetMessages(
                pullMessageRequest.MessageQueue.Topic,
                pullMessageRequest.MessageQueue.QueueId,
                pullMessageRequest.QueueOffset,
                pullMessageRequest.PullMessageBatchSize);

            if (messages.Count() > 0)
            {
                SendRemotingResponse(pullRequest, BuildFoundResponse(pullRequest.RemotingRequestSequence, messages));
                return;
            }

            var queueMinOffset = _messageService.GetQueueMinOffset(pullMessageRequest.MessageQueue.Topic, pullMessageRequest.MessageQueue.QueueId);
            if (queueMinOffset > pullMessageRequest.QueueOffset)
            {
                SendRemotingResponse(pullRequest, BuildNextOffsetResetResponse(pullRequest.RemotingRequestSequence, queueMinOffset));
            }
            else
            {
                SendRemotingResponse(pullRequest, BuildNoNewMessageResponse(pullRequest.RemotingRequestSequence));
            }
        }
        private void ExecuteReplacedPullRequest(PullRequest pullRequest)
        {
            if (IsPullRequestValid(pullRequest))
            {
                SendRemotingResponse(pullRequest, BuildIgnoredResponse(pullRequest.RemotingRequestSequence));
            }
        }
        private bool IsPullRequestValid(PullRequest pullRequest)
        {
            var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(pullRequest.PullMessageRequest.ConsumerGroup);
            return consumerGroup != null && consumerGroup.IsConsumerActive(pullRequest.RequestHandlerContext.Channel.RemoteEndPoint.ToString());
        }
        private RemotingResponse BuildNoNewMessageResponse(long requestSequence)
        {
            return new RemotingResponse((int)PullStatus.NoNewMessage, requestSequence, _emptyResponseData);
        }
        private RemotingResponse BuildIgnoredResponse(long requestSequence)
        {
            return new RemotingResponse((int)PullStatus.Ignored, requestSequence, _emptyResponseData);
        }
        private RemotingResponse BuildNextOffsetResetResponse(long requestSequence, long nextOffset)
        {
            return new RemotingResponse((int)PullStatus.NextOffsetReset, requestSequence, _binarySerializer.Serialize(new PullMessageResponse(new QueueMessage[0], nextOffset)));
        }
        private RemotingResponse BuildFoundResponse(long requestSequence, IEnumerable<QueueMessage> messages)
        {
            return new RemotingResponse((int)PullStatus.Found, requestSequence, _binarySerializer.Serialize(new PullMessageResponse(messages)));
        }
        private void SendRemotingResponse(PullRequest pullRequest, RemotingResponse remotingResponse)
        {
            pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
        }
    }
}
