using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EQueue.Common;
using EQueue.Common.Logging;

namespace EQueue.Clients.Consumers
{
    public class Consumer : IConsumer
    {
        private readonly Client _client;
        private readonly IMessageHandler _messageHandler;
        private readonly IOffsetStore _offsetStore;
        private readonly IRebalanceService _rebalanceService;
        private readonly ILogger _logger;

        public string GroupName { get; private set; }
        public MessageModel MessageModel { get; private set; }

        public IEnumerable<string> SubscriptionTopics
        {
            get { return _rebalanceService.SubscriptionTopics; }
        }

        public Consumer(string groupName, MessageModel messageModel, Client client, IRebalanceService rebalanceService, IMessageHandler messageHandler, IOffsetStore offsetStore, ILoggerFactory loggerFactory)
        {
            GroupName = groupName;
            MessageModel = messageModel;
            _client = client;
            _rebalanceService = rebalanceService;
            _messageHandler = messageHandler;
            _offsetStore = offsetStore;
            _logger = loggerFactory.Create(GetType().Name);
        }

        public void Start()
        {
            _logger.Info("consumer started...");
        }
        public void Subscribe(string topic)
        {
            _rebalanceService.RegisterSubscriptionTopic(topic);
        }
        public void Shutdown()
        {
            //TODO
        }
        public void PullMessage(PullRequest pullRequest)
        {
            StartPullMessageTask(pullRequest).ContinueWith((task) => ProcessPullResult(pullRequest, task.Result));
        }
        public void UpdateTopicSubscribeInfo(string topic, IEnumerable<MessageQueue> messageQueues)
        {
            _rebalanceService.UpdateTopicSubscribeInfo(topic, messageQueues);
        }
        public bool IsSubscribeTopicNeedUpdate(string topic)
        {
            return !_rebalanceService.SubscriptionTopics.Any(x => x == topic);
        }
        public void DoRebalance()
        {
            _rebalanceService.Rebalance();
        }
        public void PersistOffset()
        {
            foreach (var messageQueue in _rebalanceService.ProcessingMessageQueues)
            {
                try
                {
                    _offsetStore.Persist(messageQueue);
                }
                catch (Exception ex)
                {
                    _logger.Error("PersistOffset exception.", ex);
                }
            }
        }

        private Task<PullResult> StartPullMessageTask(PullRequest pullRequest)
        {
            //TODO
            return null;
        }
        private void ProcessPullResult(PullRequest pullRequest, PullResult pullResult)
        {
            pullRequest.NextOffset = pullResult.NextBeginOffset;
            pullRequest.ProcessQueue.AddMessages(pullResult.Messages);
            StartConsumeTask(pullRequest, pullResult);
            _client.EnqueuePullRequest(pullRequest);
        }
        private void StartConsumeTask(PullRequest pullRequest, PullResult pullResult)
        {
            Task.Factory.StartNew(() =>
            {
                foreach (var message in pullResult.Messages)
                {
                    try
                    {
                        _messageHandler.Handle(message);
                    }
                    catch { }  //TODO,处理失败的消息放到本地队列继续重试消费
                }
                long offset = pullRequest.ProcessQueue.RemoveMessages(pullResult.Messages);
                if (offset >= 0)
                {
                    _offsetStore.UpdateOffset(pullRequest.MessageQueue, offset);
                }
            });
        }
    }
}
