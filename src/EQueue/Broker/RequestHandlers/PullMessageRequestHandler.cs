using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.Storage;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.Processors
{
    public class PullMessageRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private SuspendedPullRequestManager _suspendedPullRequestManager;
        private IMessageStore _messageStore;
        private IQueueStore _queueStore;
        private IOffsetStore _offsetStore;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;
        private readonly byte[] EmptyResponseData;

        public PullMessageRequestHandler()
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _offsetStore = ObjectContainer.Resolve<IOffsetStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            EmptyResponseData = new byte[0];
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
                return BuildNextOffsetResetResponse(remotingRequest, nextConsumeOffset);
            }

            //尝试拉取消息
            var pullResult = PullMessages(topic, queueId, pullOffset, request.PullMessageBatchSize);

            //处理消息拉取结果
            if (pullResult.Status == PullStatus.Found)
            {
                return BuildFoundResponse(remotingRequest, pullResult.Messages);
            }
            else if (pullResult.Status == PullStatus.NextOffsetReset)
            {
                return BuildNextOffsetResetResponse(remotingRequest, pullResult.NextBeginOffset);
            }
            else if (pullResult.Status == PullStatus.QueueNotExist)
            {
                return BuildQueueNotExistResponse(remotingRequest);
            }
            else if (pullResult.Status == PullStatus.NoNewMessage)
            {
                if (request.SuspendPullRequestMilliseconds > 0)
                {
                    var pullRequest = new PullRequest(
                        remotingRequest,
                        request,
                        context,
                        DateTime.Now,
                        request.SuspendPullRequestMilliseconds,
                        ExecutePullRequest,
                        ExecutePullRequest,
                        ExecuteReplacedPullRequest);
                    _suspendedPullRequestManager.SuspendPullRequest(pullRequest);
                    return null;
                }
                return BuildNoNewMessageResponse(remotingRequest);
            }
            else
            {
                throw new Exception("Invalid pull result status.");
            }
        }

        private PullMessageResult PullMessages(string topic, int queueId, long pullOffset, int maxPullSize)
        {
            //先找到队列
            var queue = _queueStore.GetQueue(topic, queueId);
            if (queue == null)
            {
                return new PullMessageResult
                {
                    Status = PullStatus.QueueNotExist
                };
            }

            //尝试拉取消息
            var queueMaxOffset = _queueStore.GetQueueLastFlushedOffset(topic, queueId);
            var messages = new List<byte[]>();
            var currentQueueOffset = pullOffset;
            while (currentQueueOffset <= queueMaxOffset && messages.Count < maxPullSize)
            {
                try
                {
                    var messagePosition = queue.GetMessagePosition(currentQueueOffset);

                    //这里减1是因为queue中存放的消息的position是实际的position加1
                    var message = _messageStore.GetMessage(messagePosition - 1);
                    messages.Add(message);

                    currentQueueOffset++;
                }
                catch (ChunkNotExistException)
                {
                    _logger.InfoFormat("Chunk not exist, topic: {0}, queueId: {1}, currentQueueOffset: {2}", topic, queueId, currentQueueOffset);
                }
                catch (InvalidReadException ex)
                {
                    _logger.InfoFormat("Chunk read failed, topic: {0}, queueId: {1}, currentQueueOffset: {2}, errorMsg: {3}", topic, queueId, currentQueueOffset, ex.Message);
                    throw;
                }
            }

            //如果拉取到至少一个消息，则直接返回即可；
            if (messages.Count > 0)
            {
                return new PullMessageResult
                {
                    Status = PullStatus.Found,
                    Messages = messages
                };
            }

            //如果没有拉取到消息，则继续判断pullOffset的位置是否合法，分析没有拉取到消息的原因

            //pullOffset太小
            var queueMinOffset = _queueStore.GetQueueMinOffset(topic, queueId);
            if (pullOffset < queueMinOffset)
            {
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = queueMinOffset
                };
            }
            //pullOffset太大
            else if (pullOffset > queueMaxOffset + 1)
            {
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = queueMaxOffset + 1
                };
            }
            //如果正好等于queueMaxOffset+1，属于正常情况，表示当前队列没有新消息，告诉客户端没有新消息即可
            else if (pullOffset == queueMaxOffset + 1)
            {
                return new PullMessageResult
                {
                    Status = PullStatus.NoNewMessage
                };
            }
            //到这里，说明当前的pullOffset在队列的正常范围，即：>=queueMinOffset and <= queueMaxOffset；
            //正常情况，应该有消息可以返回，除非消息或消息索引所在的物理文件被删除了或正在被删除；
            else
            {
                var firstOffsetOfNextChunk = queue.GetNextChunkFirstOffset(currentQueueOffset);
                if (firstOffsetOfNextChunk < 0L)
                {
                    firstOffsetOfNextChunk = 0L;
                }
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = firstOffsetOfNextChunk
                };
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
            var pullResult = PullMessages(topic, queueId, pullOffset, pullMessageRequest.PullMessageBatchSize);
            var remotingRequest = pullRequest.RemotingRequest;

            if (pullResult.Status == PullStatus.Found)
            {
                SendRemotingResponse(pullRequest, BuildFoundResponse(pullRequest.RemotingRequest, pullResult.Messages));
            }
            else if (pullResult.Status == PullStatus.NextOffsetReset)
            {
                SendRemotingResponse(pullRequest, BuildNextOffsetResetResponse(remotingRequest, pullResult.NextBeginOffset));
            }
            else if (pullResult.Status == PullStatus.QueueNotExist)
            {
                SendRemotingResponse(pullRequest, BuildQueueNotExistResponse(remotingRequest));
            }
            else if (pullResult.Status == PullStatus.NoNewMessage)
            {
                SendRemotingResponse(pullRequest, BuildNoNewMessageResponse(pullRequest.RemotingRequest));
            }
        }
        private void ExecuteReplacedPullRequest(PullRequest pullRequest)
        {
            if (!IsPullRequestValid(pullRequest))
            {
                return;
            }
            SendRemotingResponse(pullRequest, BuildIgnoredResponse(pullRequest.RemotingRequest));
        }
        private bool IsPullRequestValid(PullRequest pullRequest)
        {
            try
            {
                return _consumerManager.IsConsumerActive(pullRequest.PullMessageRequest.ConsumerGroup, pullRequest.PullMessageRequest.ConsumerId);
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        }
        private RemotingResponse BuildNoNewMessageResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.NoNewMessage);
        }
        private RemotingResponse BuildIgnoredResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.Ignored);
        }
        private RemotingResponse BuildNextOffsetResetResponse(RemotingRequest remotingRequest, long nextOffset)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.NextOffsetReset, BitConverter.GetBytes(nextOffset));
        }
        private RemotingResponse BuildQueueNotExistResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.QueueNotExist);
        }
        private RemotingResponse BuildFoundResponse(RemotingRequest remotingRequest, IEnumerable<byte[]> messages)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.Found, Combine(messages));
        }
        private void SendRemotingResponse(PullRequest pullRequest, RemotingResponse remotingResponse)
        {
            pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
        }
        private long GetNextConsumeOffset(string topic, int queueId, string consumerGroup, ConsumeFromWhere consumerFromWhere)
        {
            var lastConsumedQueueOffset = _offsetStore.GetConsumeOffset(topic, queueId, consumerGroup);
            if (lastConsumedQueueOffset >= 0)
            {
                var queueCurrentOffset = _queueStore.GetQueueLastFlushedOffset(topic, queueId);
                return queueCurrentOffset < lastConsumedQueueOffset ? queueCurrentOffset + 1 : lastConsumedQueueOffset + 1;
            }

            if (consumerFromWhere == ConsumeFromWhere.FirstOffset)
            {
                var queueMinOffset = _queueStore.GetQueueMinOffset(topic, queueId);
                if (queueMinOffset < 0)
                {
                    queueMinOffset = 0;
                }
                return queueMinOffset;
            }
            else
            {
                var queueCurrentOffset = _queueStore.GetQueueLastFlushedOffset(topic, queueId);
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
        private static byte[] Combine(IEnumerable<byte[]> arrays)
        {
            byte[] destination = new byte[arrays.Sum(x => x.Length)];
            int offset = 0;
            foreach (byte[] data in arrays)
            {
                Buffer.BlockCopy(data, 0, destination, offset, data.Length);
                offset += data.Length;
            }
            return destination;
        }
    }
}
