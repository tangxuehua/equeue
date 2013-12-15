using System;
using System.Collections.Generic;
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
        private readonly ILogger _logger;

        public string GroupName
        {
            get { throw new NotImplementedException(); }
        }

        public MessageModel MessageModel
        {
            get { throw new NotImplementedException(); }
        }

        public IEnumerable<string> SubscriptionTopics
        {
            get { throw new NotImplementedException(); }
        }

        public Consumer(Client client, IMessageHandler messageHandler, IOffsetStore offsetStore, ILoggerFactory loggerFactory)
        {
            _client = client;
            _messageHandler = messageHandler;
            _offsetStore = offsetStore;
            _logger = loggerFactory.Create(GetType().Name);
        }

        public virtual void Start()
        {
            _logger.Info("consumer started...");
        }
        public virtual void Shutdown()
        {
            //TODO
        }

        public void PullMessage(PullRequest pullRequest)
        {
            StartPullMessageTask(pullRequest).ContinueWith((task) => ProcessPullResult(pullRequest, task.Result));
        }

        public void UpdateTopicSubscribeInfo(string topic, IEnumerable<MessageQueue> messageQueues)
        {
            throw new NotImplementedException();
        }

        public bool IsSubscribeTopicNeedUpdate(string topic)
        {
            throw new NotImplementedException();
        }

        public void DoRebalance()
        {
            throw new NotImplementedException();
        }

        public void PersistOffset()
        {
            throw new NotImplementedException();
        }

        private Task<PullResult> StartPullMessageTask(PullRequest pullRequest)
        {
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
