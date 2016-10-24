using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;

namespace EQueue.Clients.Consumers
{
    public class RebalanceService
    {
        #region Private Variables

        private readonly string _clientId;
        private readonly Consumer _consumer;
        private readonly ClientService _clientService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ConcurrentDictionary<string, PullRequest> _pullRequestDict;
        private readonly CommitConsumeOffsetService _commitConsumeOffsetService;
        private readonly PullMessageService _pullMessageService;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        #endregion

        public RebalanceService(Consumer consumer, ClientService clientService, PullMessageService pullMessageService, CommitConsumeOffsetService commitConsumeOffsetService)
        {
            _consumer = consumer;
            _clientService = clientService;
            _clientId = clientService.GetClientId();
            _pullRequestDict = new ConcurrentDictionary<string, PullRequest>();
            _allocateMessageQueueStragegy = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _pullMessageService = pullMessageService;
            _commitConsumeOffsetService = commitConsumeOffsetService;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _scheduleService.StartTask("Rebalance", Rebalance, 1000, _consumer.Setting.RebalanceInterval);
            if (_consumer.Setting.AutoPull)
            {
                _scheduleService.StartTask("CommitOffsets", CommitOffsets, 1000, _consumer.Setting.CommitConsumerOffsetInterval);
            }
            _logger.InfoFormat("{0} startted.", GetType().Name);
        }
        public void Stop()
        {
            _scheduleService.StopTask("Rebalance");
            if (_consumer.Setting.AutoPull)
            {
                _scheduleService.StopTask("CommitOffsets");
            }
            foreach (var pullRequest in _pullRequestDict.Values)
            {
                pullRequest.IsDropped = true;
            }
            _pullRequestDict.Clear();
            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }
        public IEnumerable<MessageQueueEx> GetCurrentQueues()
        {
            return _pullRequestDict.Values.Select(x =>
            {
                return new MessageQueueEx(x.MessageQueue.BrokerName, x.MessageQueue.Topic, x.MessageQueue.QueueId)
                {
                    ClientCachedMessageCount = x.ProcessQueue.GetMessageCount()
                };
            }).ToList();
        }

        private void Rebalance()
        {
            foreach (var pair in _consumer.SubscriptionTopics)
            {
                var topic = pair.Key;
                try
                {
                    RebalanceClustering(pair);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("RebalanceClustering has exception, consumerGroup: {0}, consumerId: {1}, topic: {2}", _consumer.GroupName, _clientId, topic), ex);
                }
            }
        }
        private void RebalanceClustering(KeyValuePair<string, HashSet<string>> pair)
        {
            var topic = pair.Key;
            try
            {
                var consumerIdList = GetConsumerIdsForTopic(topic);
                if (consumerIdList == null || consumerIdList.Count == 0)
                {
                    _logger.WarnFormat("No available consumers found.");
                    UpdatePullRequestDict(pair, new List<MessageQueue>());
                    return;
                }
                var messageQueueList = _clientService.GetTopicMessageQueues(topic);
                if (messageQueueList == null || messageQueueList.Count == 0)
                {
                    _logger.WarnFormat("No available message queues found.");
                    UpdatePullRequestDict(pair, new List<MessageQueue>());
                    return;
                }
                var allocatedMessageQueueList = _allocateMessageQueueStragegy.Allocate(_clientId, messageQueueList, consumerIdList).ToList();
                UpdatePullRequestDict(pair, allocatedMessageQueueList);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("RebalanceClustering has exception, consumerGroup: {0}, consumerId: {1}, topic: {2}", _consumer.GroupName, _clientId, topic), ex);
                UpdatePullRequestDict(pair, new List<MessageQueue>());
            }
        }
        private IList<string> GetConsumerIdsForTopic(string topic)
        {
            var brokerConnection = _clientService.GetFirstBrokerConnection();
            var request = _binarySerializer.Serialize(new GetConsumerIdsForTopicRequest(_consumer.GroupName, topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetConsumerIdsForTopic, request);
            var remotingResponse = brokerConnection.AdminRemotingClient.InvokeSync(remotingRequest, 1000 * 5);
            if (remotingResponse.ResponseCode != ResponseCode.Success)
            {
                throw new Exception(string.Format("GetConsumerIdsForTopic has exception, consumerGroup: {0}, topic: {1}, brokerAddress: {2}, remoting response code: {3}, errorMessage: {4}",
                    _consumer.GroupName,
                    topic,
                    brokerConnection.AdminRemotingClient.ServerEndPoint.ToAddress(),
                    remotingResponse.ResponseCode,
                    Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
            }

            var consumerIds = Encoding.UTF8.GetString(remotingResponse.ResponseBody);
            var consumerIdList = consumerIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).ToList();
            consumerIdList.Sort();
            return consumerIdList;
        }
        private void UpdatePullRequestDict(KeyValuePair<string, HashSet<string>> pair, IList<MessageQueue> messageQueues)
        {
            var topic = pair.Key;
            var toRemovePullRequestKeys = new List<string>();
            foreach (var pullRequest in _pullRequestDict.Values.Where(x => x.MessageQueue.Topic == topic))
            {
                var key = pullRequest.MessageQueue.ToString();
                if (!messageQueues.Any(x => x.ToString() == key))
                {
                    toRemovePullRequestKeys.Add(key);
                }
            }
            foreach (var pullRequestKey in toRemovePullRequestKeys)
            {
                PullRequest pullRequest;
                if (_pullRequestDict.TryRemove(pullRequestKey, out pullRequest))
                {
                    pullRequest.IsDropped = true;
                    _commitConsumeOffsetService.CommitConsumeOffset(pullRequest);
                    _logger.InfoFormat("Dropped pull request, consumerGroup: {0}, consumerId: {1}, queue: {2}, tags: {3}",
                        _consumer.GroupName,
                        _clientId,
                        pullRequest.MessageQueue,
                        string.Join("|", pullRequest.Tags));
                }
            }
            foreach (var messageQueue in messageQueues)
            {
                var key = messageQueue.ToString();
                PullRequest exist;
                if (!_pullRequestDict.TryGetValue(key, out exist))
                {
                    var pullRequest = new PullRequest(_clientId, _consumer.GroupName, messageQueue, -1, pair.Value);
                    if (_pullRequestDict.TryAdd(key, pullRequest))
                    {
                        _pullMessageService.SchedulePullRequest(pullRequest);
                        _logger.InfoFormat("Added pull request, consumerGroup: {0}, consumerId: {1}, queue: {2}, tags: {3}",
                            _consumer.GroupName,
                            _clientId,
                            pullRequest.MessageQueue,
                            string.Join("|", pullRequest.Tags));
                    }
                }
            }
        }
        private void CommitOffsets()
        {
            foreach (var pullRequest in _pullRequestDict.Values)
            {
                _commitConsumeOffsetService.CommitConsumeOffset(pullRequest);
            }
        }
    }
}
