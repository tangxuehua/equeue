using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using EQueue.Common;
using EQueue.Common.Logging;

namespace EQueue.Client.Consumer
{
    public class RebalanceService
    {
        private readonly ConcurrentDictionary<MessageQueue, ProcessQueue> _processQueueDict = new ConcurrentDictionary<MessageQueue, ProcessQueue>();
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicSubscribeInfoDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
        private readonly ConcurrentDictionary<string, IEnumerable<string>> _subscriptionDict = new ConcurrentDictionary<string, IEnumerable<string>>();
        private readonly ILogger _logger;
        private readonly DefaultClient _client;
        private readonly string _consumerGroup;
        private readonly MessageModel _messageModel;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly IOffsetStore _offsetStore;

        public RebalanceService(
            DefaultClient client,
            string consumerGroup,
            MessageModel messageModel,
            IAllocateMessageQueueStrategy allocateMessageQueueStrategy,
            IOffsetStore offsetStore,
            ILoggerFactory loggerFactory)
        {
            _client = client;
            _consumerGroup = consumerGroup;
            _messageModel = messageModel;
            _allocateMessageQueueStragegy = allocateMessageQueueStrategy;
            _offsetStore = offsetStore;
            _logger = loggerFactory.Create(GetType().Name);
        }

        public void Rebalance()
        {
            if (_messageModel == MessageModel.BROADCASTING)
            {
                foreach (var topic in _subscriptionDict.Keys)
                {
                    try
                    {
                        RebalanceBroadCasting(topic);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error("RebalanceBroadCasting has exception", ex);
                    }
                }
            }
            else if (_messageModel == MessageModel.CLUSTERING)
            {
                foreach (var topic in _subscriptionDict.Keys)
                {
                    try
                    {
                        RebalanceClustering(topic);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error("RebalanceClustering has exception", ex);
                    }
                }
            }

            TruncateMessageQueueNotMyTopic();
        }

        private void RebalanceBroadCasting(string topic)
        {
            if (_topicSubscribeInfoDict.ContainsKey(topic))
            {
                var messageQueues = _topicSubscribeInfoDict[topic];
                var changed = UpdateProcessQueueDict(topic, messageQueues);
                if (changed)
                {
                    _logger.InfoFormat("messageQueueChanged [consumerGroup:{0}, topic:{1}, allocatedMessageQueues:{2}]", _consumerGroup, topic, string.Join("|", messageQueues));
                }
            }
            else
            {
                _logger.WarnFormat("DoRebalance of broad casting, consumerGroup: {0}, but the topic[{1}] not exist.", _consumerGroup, topic);
            }
        }
        private void RebalanceClustering(string topic)
        {
            if (_topicSubscribeInfoDict.ContainsKey(topic))
            {
                var messageQueues = _topicSubscribeInfoDict[topic];
                var consumerIds = _client.FindConsumerIdList(_consumerGroup);

                var messageQueueList = messageQueues.ToList();
                var consumerIdList = consumerIds.ToList();
                messageQueueList.Sort();
                consumerIdList.Sort();

                IEnumerable<MessageQueue> allocatedMessageQueues = new List<MessageQueue>();
                try
                {
                    allocatedMessageQueues = _allocateMessageQueueStragegy.Allocate(_client.ClientId, messageQueueList, consumerIdList);
                }
                catch (Exception ex)
                {
                    _logger.Error("Allocate message queue has exception.", ex);
                }

                var allocatedMessageQueueList = allocatedMessageQueues.ToList();
                var changed = UpdateProcessQueueDict(topic, allocatedMessageQueueList);
                if (changed)
                {
                    _logger.InfoFormat("messageQueueChanged [consumerGroup:{0}, topic:{1}, allocatedMessageQueues:{2}, consumerIds:{3}]", _consumerGroup, topic, string.Join("|", allocatedMessageQueueList), string.Join("|", consumerIdList));
                }
            }
            else
            {
                _logger.WarnFormat("DoRebalance of clustering, consumerGroup: {0}, but the topic[{1}] not exist.", _consumerGroup, topic);
            }
        }
        private bool UpdateProcessQueueDict(string topic, IList<MessageQueue> messageQueues)
        {
            var changed = false;

            foreach (var messageQueue in _processQueueDict.Keys)
            {
                if (messageQueue.Topic == topic)
                {
                    if (!messageQueues.Contains(messageQueue))
                    {
                        changed = true;
                        ProcessQueue processQueue;
                        _processQueueDict.TryRemove(messageQueue, out processQueue);
                        RemoveUnnecessaryMessageQueue(messageQueue, processQueue);
                    }
                }
            }

            var pullRequestList = new List<PullRequest>();
            foreach (var messageQueue in messageQueues)
            {
                if (!_processQueueDict.ContainsKey(messageQueue))
                {
                    var pullRequest = new PullRequest();
                    pullRequest.ConsumerGroup = _consumerGroup;
                    pullRequest.MessageQueue = messageQueue;
                    pullRequest.ProcessQueue = new ProcessQueue();

                    long nextOffset = ComputePullFromWhere(messageQueue);
                    if (nextOffset >= 0)
                    {
                        changed = true;
                        pullRequest.NextOffset = nextOffset;
                        pullRequestList.Add(pullRequest);
                        _processQueueDict.TryAdd(messageQueue, pullRequest.ProcessQueue);
                        _logger.InfoFormat("DoRebalance, ConsumerGroup: {0}, Add a new messageQueue, {1}", _consumerGroup, messageQueue);
                    }
                    else
                    {
                        _logger.WarnFormat("DoRebalance, ConsumerGroup: {0}, The new messageQueue {1} cannot be added as the nextOffset is < 0.", _consumerGroup, messageQueue);
                    }
                }
            }

            DispatchPullRequest(pullRequestList);

            return changed;
        }
        private void TruncateMessageQueueNotMyTopic()
        {
            var shouldRemoveQueues = new List<MessageQueue>();
            foreach (var messageQueue in _processQueueDict.Keys)
            {
                if (!_subscriptionDict.ContainsKey(messageQueue.Topic))
                {
                    shouldRemoveQueues.Add(messageQueue);
                }
            }
            foreach (var queue in shouldRemoveQueues)
            {
                ProcessQueue removedQueue;
                _processQueueDict.TryRemove(queue, out removedQueue);
            }
        }
        private void RemoveUnnecessaryMessageQueue(MessageQueue messageQueue, ProcessQueue processQueue)
        {
            _offsetStore.Persist(messageQueue);
            _offsetStore.RemoveOffset(messageQueue);
        }
        private void DispatchPullRequest(IEnumerable<PullRequest> pullRequestList)
        {
            foreach (var pullRequest in pullRequestList)
            {
                _client.EnqueuePullRequest(pullRequest);
                _logger.InfoFormat("doRebalance, consumerGroup:{0}, add a new pull request {1}", _consumerGroup, pullRequest);
            }
        }
        private long ComputePullFromWhere(MessageQueue messageQueue)
        {
            var offset = -1L;

            var lastOffset = _offsetStore.ReadOffset(messageQueue, OffsetReadType.ReadFromStore);
            if (lastOffset >= 0)
            {
                offset = lastOffset;
            }
            else if (lastOffset == -1)
            {
                offset = long.MaxValue;
            }

            return offset;
        }
    }
}
