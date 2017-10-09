using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Storage.Exceptions;
using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Protocols;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers
{
    public class PullMessageRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private SuspendedPullRequestManager _suspendedPullRequestManager;
        private IMessageStore _messageStore;
        private IQueueStore _queueStore;
        private IConsumeOffsetStore _offsetStore;
        private ILogger _logger;

        public PullMessageRequestHandler()
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                return BuildBrokerIsCleaningResponse(remotingRequest);
            }

            var request = DeserializePullMessageRequest(remotingRequest.Body);
            var topic = request.MessageQueue.Topic;
            var tags = request.Tags;
            var queueId = request.MessageQueue.QueueId;
            var pullOffset = request.QueueOffset;

            //如果消费者第一次过来拉取消息，则计算下一个应该拉取的位置，并返回给消费者
            var nextConsumeOffset = 0L;
            if (pullOffset < 0)
            {
                nextConsumeOffset = GetNextConsumeOffset(topic, queueId, request.ConsumerGroup, request.ConsumeFromWhere);
                return BuildNextOffsetResetResponse(remotingRequest, nextConsumeOffset);
            }
            //如果用户人工指定了下次要拉取的位置，则返回该位置给消费者并清除该指定的位置
            else if (_offsetStore.TryFetchNextConsumeOffset(topic, queueId, request.ConsumerGroup, out nextConsumeOffset))
            {
                return BuildNextOffsetResetResponse(remotingRequest, nextConsumeOffset);
            }

            //尝试拉取消息
            var pullResult = PullMessages(topic, tags, queueId, pullOffset, request.PullMessageBatchSize);

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
                        ExecuteNoNewMessagePullRequest,
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

        private PullMessageResult PullMessages(string topic, string tags, int queueId, long pullOffset, int maxPullSize)
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
            var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topic, queueId);
            var messages = new List<byte[]>();
            var queueOffset = pullOffset;
            var messageChunkNotExistException = default(ChunkNotExistException);

            while (queueOffset <= queueCurrentOffset && messages.Count < maxPullSize)
            {
                long messagePosition = -1;
                int tagCode;

                //先获取消息的位置
                try
                {
                    messagePosition = queue.GetMessagePosition(queueOffset, out tagCode);
                }
                catch (ChunkNotExistException)
                {
                    break;
                }
                catch (ChunkReadException ex)
                {
                    _logger.Error(string.Format("Queue chunk read failed, topic: {0}, queueId: {1}, queueOffset: {2}", topic, queueId, queueOffset), ex);
                    throw;
                }

                //再获取消息
                if (messagePosition < 0)
                {
                    break;
                }
                try
                {
                    var message = _messageStore.GetMessageBuffer(messagePosition);
                    if (message == null)
                    {
                        break;
                    }

                    if (string.IsNullOrEmpty(tags) || tags == "*")
                    {
                        messages.Add(message);
                    }
                    else
                    {
                        var tagList = tags.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (var tag in tagList)
                        {
                            if (tag == "*" || tag.GetStringHashcode() == tagCode)
                            {
                                messages.Add(message);
                                break;
                            }
                        }
                    }

                    queueOffset++;
                }
                catch (ChunkNotExistException ex)
                {
                    messageChunkNotExistException = ex;
                    break;
                }
                catch (ChunkReadException ex)
                {
                    //遇到这种异常，说明某个消息在队列(queue chunk)中存在，但在message chunk中不存在；
                    //出现这种现象的原因是由于，默认情况下，message chunk, queue chunk都是异步持久化的，默认定时100ms刷盘一次；
                    //所以，当broker正好在被关闭的时刻，假如queue chunk刷盘成功了，而对应的消息在message chunk中还未来得及刷盘，那就意味着这部分消息就丢失了；
                    //那当broker下次重启后，丢失的那些消息就找不到了，无法被消费；所以当Consumer拉取这些消息时，就会抛这个异常；
                    //这种情况下，重试拉取已经没有意义，故我们能做的是记录错误日志，记录下来哪个topic下的哪个queue的哪个位置的消息找不到了；这样我们就知道哪些消息丢失了；
                    //然后我们继续拉取该队列的后续的消息。
                    //如果大家希望避免这种问题，如果你的业务场景消息量不大或者使用了SSD硬盘，则建议你使用同步算盘的方式，这样可以确保消息不丢，
                    //大家通过配置ChunkManagerConfig.SyncFlush=true来实现；
                    _logger.Error(string.Format("Message chunk read failed, topic: {0}, queueId: {1}, queueOffset: {2}", topic, queueId, queueOffset), ex);
                    queueOffset++;
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
                _logger.InfoFormat("Reset next pullOffset to queueMinOffset, [topic: {0}, queueId: {1}, pullOffset: {2}, queueMinOffset: {3}]", topic, queueId, pullOffset, queueMinOffset);
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = queueMinOffset
                };
            }
            //pullOffset太大
            else if (pullOffset > queueCurrentOffset + 1)
            {
                _logger.InfoFormat("Reset next pullOffset to queueCurrentOffset, [topic: {0}, queueId: {1}, pullOffset: {2}, queueCurrentOffset: {3}]", topic, queueId, pullOffset, queueCurrentOffset);
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = queueCurrentOffset + 1
                };
            }
            //如果当前的pullOffset对应的Message的Chunk文件不存在，则需要重新计算下一个pullOffset
            else if (messageChunkNotExistException != null)
            {
                var nextPullOffset = CalculateNextPullOffset(queue, pullOffset, queueCurrentOffset);
                _logger.InfoFormat("Reset next pullOffset to calculatedNextPullOffset, [topic: {0}, queueId: {1}, pullOffset: {2}, calculatedNextPullOffset: {3}]", topic, queueId, pullOffset, nextPullOffset);
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = nextPullOffset
                };
            }
            //到这里，说明当前的pullOffset在队列的正确范围，即：>=queueMinOffset and <= queueCurrentOffset；
            //但如果还是没有拉取到消息，则可能的情况有：
            //1）要拉取的消息还未来得及刷盘；
            //2）要拉取的消息对应的Queue的Chunk文件被删除了；
            //不管是哪种情况，告诉Consumer没有新消息即可。如果是情况1，则也许下次PullRequest就会拉取到消息；如果是情况2，则下次PullRequest就会被判断出pullOffset过小
            else
            {
                return new PullMessageResult
                {
                    Status = PullStatus.NoNewMessage
                };
            }
        }
        private long CalculateNextPullOffset(Queue queue, long pullOffset, long queueCurrentOffset)
        {
            var queueOffset = pullOffset + 1;
            while (queueOffset <= queueCurrentOffset)
            {
                int tagCode;
                var messagePosition = queue.GetMessagePosition(queueOffset, out tagCode);
                if (_messageStore.IsMessagePositionExist(messagePosition))
                {
                    return queueOffset;
                }
                queueOffset++;
            }
            return queueOffset;
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
            var pullResult = PullMessages(topic, pullMessageRequest.Tags, queueId, pullOffset, pullMessageRequest.PullMessageBatchSize);
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
        private void ExecuteNoNewMessagePullRequest(PullRequest pullRequest)
        {
            if (!IsPullRequestValid(pullRequest))
            {
                return;
            }
            SendRemotingResponse(pullRequest, BuildNoNewMessageResponse(pullRequest.RemotingRequest));
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
        private RemotingResponse BuildBrokerIsCleaningResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.BrokerIsCleaning);
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
            var queueConsumedOffset = _offsetStore.GetConsumeOffset(topic, queueId, consumerGroup);
            if (queueConsumedOffset >= 0)
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topic, queueId);
                return queueCurrentOffset < queueConsumedOffset ? queueCurrentOffset + 1 : queueConsumedOffset + 1;
            }

            if (consumerFromWhere == ConsumeFromWhere.FirstOffset)
            {
                var queueMinOffset = _queueStore.GetQueueMinOffset(topic, queueId);
                if (queueMinOffset < 0)
                {
                    return 0;
                }
                return queueMinOffset;
            }
            else
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topic, queueId);
                if (queueCurrentOffset < 0)
                {
                    return 0;
                }
                return queueCurrentOffset + 1;
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

        class PullMessageResult
        {
            public PullStatus Status { get; set; }
            public long NextBeginOffset { get; set; }
            public IEnumerable<byte[]> Messages { get; set; }
        }
    }
}
