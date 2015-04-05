using System;
using System.Collections.Generic;
using System.IO;
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

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = DeserializePullMessageRequest(remotingRequest.Body);
            var topic = request.MessageQueue.Topic;
            var queueId = request.MessageQueue.QueueId;
            var pullOffset = request.QueueOffset;

            //如果消费者第一次过来拉取消息，则计算下一个应该拉取的位置，并返回给消费者
            if (pullOffset < 0)
            {
                var nextConsumeOffset = GetNextConsumeOffset(topic, queueId, request.ConsumerGroup, request.ConsumeFromWhere);
                return BuildNextOffsetResetResponse(remotingRequest.Sequence, nextConsumeOffset);
            }

            //尝试拉取消息
            var messages = _messageService.GetMessages(topic, queueId, pullOffset, request.PullMessageBatchSize);

            //如果消息存在，则返回消息
            if (messages.Count() > 0)
            {
                return BuildFoundResponse(remotingRequest.Sequence, messages);
            }

            //消息不存在，如果挂起时间大于0，则挂起请求
            if (request.SuspendPullRequestMilliseconds > 0)
            {
                var pullRequest = new PullRequest(
                    remotingRequest.Sequence,
                    request,
                    context,
                    DateTime.Now,
                    request.SuspendPullRequestMilliseconds,
                    ExecutePullRequest,
                    ExecutePullRequest,
                    ExecuteReplacedPullRequest);
                _brokerController.SuspendedPullRequestManager.SuspendPullRequest(pullRequest);
                return null;
            }

            //获取队列的第一个消息以及最后一个消息的offset
            var queueMinOffset = _messageService.GetQueueMinOffset(topic, queueId);
            var queueCurrentOffset = _messageService.GetQueueCurrentOffset(topic, queueId);

            //如果pullOffset比队列的第一个消息的offset还要小或者比队列的最后一个消息的offset+1还要大，则将pullOffset重置为第一个消息的offset
            if (pullOffset < queueMinOffset || pullOffset > queueCurrentOffset + 1)
            {
                return BuildNextOffsetResetResponse(remotingRequest.Sequence, queueMinOffset);
            }
            else
            {
                return BuildNoNewMessageResponse(remotingRequest.Sequence);
            }
        }

        private void ExecutePullRequest(PullRequest pullRequest)
        {
            if (!IsPullRequestValid(pullRequest))
            {
                return;
            }

            var pullMessageRequest = pullRequest.PullMessageRequest;
            var topic = pullMessageRequest.MessageQueue.Topic;
            var queueId = pullMessageRequest.MessageQueue.QueueId;
            var pullOffset = pullMessageRequest.QueueOffset;

            var messages = _messageService.GetMessages(topic, queueId, pullOffset, pullMessageRequest.PullMessageBatchSize);

            if (messages.Count() > 0)
            {
                SendRemotingResponse(pullRequest, BuildFoundResponse(pullRequest.RemotingRequestSequence, messages));
                return;
            }

            //获取队列的第一个消息以及最后一个消息的offset
            var queueMinOffset = _messageService.GetQueueMinOffset(topic, queueId);
            var queueCurrentOffset = _messageService.GetQueueCurrentOffset(topic, queueId);

            //如果pullOffset比队列的第一个消息的offset还要小或者比队列的最后一个消息的offset+1还要大，则将pullOffset重置为第一个消息的offset
            if (pullOffset < queueMinOffset || pullOffset > queueCurrentOffset + 1)
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
        private long GetNextConsumeOffset(string topic, int queueId, string consumerGroup, ConsumeFromWhere consumerFromWhere)
        {
            var lastConsumedQueueOffset = _offsetManager.GetQueueOffset(topic, queueId, consumerGroup);
            if (lastConsumedQueueOffset >= 0)
            {
                return lastConsumedQueueOffset + 1;
            }

            if (consumerFromWhere == ConsumeFromWhere.FirstOffset)
            {
                var queueMinOffset = _messageService.GetQueueMinOffset(topic, queueId);
                if (queueMinOffset < 0)
                {
                    queueMinOffset = 0;
                }
                return queueMinOffset;
            }
            else
            {
                var queueCurrentOffset = _messageService.GetQueueCurrentOffset(topic, queueId);
                if (queueCurrentOffset < 0)
                {
                    queueCurrentOffset = 0;
                }
                else
                {
                    queueCurrentOffset++;
                }
                return queueCurrentOffset;
            }
        }
        private static PullMessageRequest DeserializePullMessageRequest(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                return PullMessageRequest.ReadFromStream(stream);
            }
        }
    }
}
