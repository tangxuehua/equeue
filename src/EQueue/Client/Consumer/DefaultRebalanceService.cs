using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        public RebalanceService(DefaultClient client, string consumerGroup, MessageModel messageModel, IAllocateMessageQueueStrategy allocateMessageQueueStrategy, ILoggerFactory loggerFactory)
        {
            _client = client;
            _consumerGroup = consumerGroup;
            _messageModel = messageModel;
            _allocateMessageQueueStragegy = allocateMessageQueueStrategy;
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
                _logger.InfoFormat("messageQueueChanged {0} {1} {2}", _consumerGroup, topic, string.Join("|", messageQueues));
            }
            else
            {
                _logger.WarnFormat("DoRebalance, consumerGroup: {0}, but the topic[{1}] not exist.", _consumerGroup, topic);
            }
        }
        private void RebalanceClustering(string topic)
        {
            //TODO
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
            //TODO
        }
        private void RemoveUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq)
        {
            //TODO
        }
        private void DispatchPullRequest(IEnumerable<PullRequest> pullRequestList)
        {
            //TODO
        }
        private long ComputePullFromWhere(MessageQueue mq)
        {
            //TODO
            return 0L;
        }
    }
}
