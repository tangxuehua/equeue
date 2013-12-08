using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EQueue.Common;
using EQueue.Common.Logging;

namespace EQueue.Client.Consumer
{
    public abstract class ConsumerBase : IConsumer
    {
        private readonly IPullMessageService _pullMessageService;
        private readonly IOffsetStore _offsetStore;
        private readonly ILogger _logger;
        private IMessageHandler _messageHandler;

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

        public ConsumerBase(IPullMessageService pullMessageService, IOffsetStore offsetStore)
        {
            _pullMessageService = pullMessageService;
            _offsetStore = offsetStore;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public virtual void Start()
        {
            _messageHandler = GetMessageHandler();

            _pullMessageService.Start();

            _logger.Info("Consumer started...");
        }

        public virtual void Shutdown()
        {
            //TODO
        }

        protected abstract IMessageHandler GetMessageHandler();

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
            _pullMessageService.ExecutePullRequestImmediately(pullRequest);
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
