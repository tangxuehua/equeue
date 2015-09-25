using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Broker.LongPolling
{
    public class SuspendedPullRequestManager
    {
        private const string Separator = "@";
        private BlockingCollection<NotifyItem> _notifyQueue = new BlockingCollection<NotifyItem>(new ConcurrentQueue<NotifyItem>());
        private readonly ConcurrentDictionary<string, PullRequest> _queueRequestDict = new ConcurrentDictionary<string, PullRequest>();
        private readonly IScheduleService _scheduleService;
        private readonly IQueueService _queueService;
        private readonly ILogger _logger;
        private TaskFactory _taskFactory;
        private Worker _notifyMessageArrivedWorker;

        public SuspendedPullRequestManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _queueService = ObjectContainer.Resolve<IQueueService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }
        public void SuspendPullRequest(PullRequest pullRequest)
        {
            var pullMessageRequest = pullRequest.PullMessageRequest;
            var key = BuildKey(pullMessageRequest.MessageQueue.Topic, pullMessageRequest.MessageQueue.QueueId, pullMessageRequest.ConsumerGroup);
            var changed = false;
            var existingRequest = default(PullRequest);

            var currentPullRequest = _queueRequestDict.AddOrUpdate(key, x =>
            {
                _logger.DebugFormat("Added new PullRequest, Id:{0}, RequestSequence:{6}, SuspendStartTime:{1}, ConsumerGroup:{2}, Topic:{3}, QueueId:{4}, QueueOffset:{5}",
                    pullRequest.Id,
                    pullRequest.SuspendStartTime,
                    pullRequest.PullMessageRequest.ConsumerGroup,
                    pullRequest.PullMessageRequest.MessageQueue.Topic,
                    pullRequest.PullMessageRequest.MessageQueue.QueueId,
                    pullRequest.PullMessageRequest.QueueOffset,
                    pullRequest.RemotingRequest.Sequence);
                return pullRequest;
            }, (x, request) =>
            {
                existingRequest = request;
                changed = true;
                return pullRequest;
            });

            CheckNewMessageExist(key, currentPullRequest.PullMessageRequest.MessageQueue.Topic, currentPullRequest.PullMessageRequest.MessageQueue.QueueId, currentPullRequest.PullMessageRequest.QueueOffset);

            if (changed && existingRequest != null)
            {
                _logger.DebugFormat("Replaced existing PullRequest, new PullRequest Id:{0}, RequestSequence:{6}, SuspendStartTime:{1}, ConsumerGroup:{2}, Topic:{3}, QueueId:{4}, QueueOffset:{5}",
                    existingRequest.Id,
                    existingRequest.SuspendStartTime,
                    existingRequest.PullMessageRequest.ConsumerGroup,
                    existingRequest.PullMessageRequest.MessageQueue.Topic,
                    existingRequest.PullMessageRequest.MessageQueue.QueueId,
                    existingRequest.PullMessageRequest.QueueOffset,
                    pullRequest.RemotingRequest.Sequence);

                var currentRequest = existingRequest;
                _taskFactory.StartNew(() => currentRequest.ReplacedAction(currentRequest));
            }
        }
        public void NotifyNewMessage(string topic, int queueId, long queueOffset)
        {
            if (BrokerController.Instance.Setting.NotifyWhenMessageArrived)
            {
                _notifyQueue.Add(new NotifyItem { Topic = topic, QueueId = queueId, QueueOffset = queueOffset });
            }
        }

        public void Start()
        {
            _queueRequestDict.Clear();
            StopCheckBlockingPullRequestTask();
            StopNotifyMessageArrivedWorker();

            _notifyQueue = new BlockingCollection<NotifyItem>(new ConcurrentQueue<NotifyItem>());
            _taskFactory = new TaskFactory(new LimitedConcurrencyLevelTaskScheduler(BrokerController.Instance.Setting.NotifyMessageArrivedThreadMaxCount));
            if (BrokerController.Instance.Setting.NotifyWhenMessageArrived)
            {
                _notifyMessageArrivedWorker = new Worker("SuspendedPullRequestManager.NotifyMessageArrived", () =>
                {
                    var notifyItem = _notifyQueue.Take();
                    if (notifyItem == null) return;
                    NotifyMessageArrived(notifyItem.Topic, notifyItem.QueueId, notifyItem.QueueOffset);
                });
            }
            StartCheckBlockingPullRequestTask();
            StartNotifyMessageArrivedWorker();
        }
        public void Shutdown()
        {
            StopCheckBlockingPullRequestTask();
            StopNotifyMessageArrivedWorker();
        }

        private void StartCheckBlockingPullRequestTask()
        {
            _scheduleService.StartTask("SuspendedPullRequestManager.CheckBlockingPullRequest", CheckBlockingPullRequest, BrokerController.Instance.Setting.CheckBlockingPullRequestMilliseconds, BrokerController.Instance.Setting.CheckBlockingPullRequestMilliseconds);
        }
        private void StopCheckBlockingPullRequestTask()
        {
            _scheduleService.StopTask("SuspendedPullRequestManager.CheckBlockingPullRequest");
        }
        private void StartNotifyMessageArrivedWorker()
        {
            if (_notifyMessageArrivedWorker != null)
            {
                _notifyMessageArrivedWorker.Start();
            }
        }
        private void StopNotifyMessageArrivedWorker()
        {
            if (_notifyMessageArrivedWorker != null)
            {
                _notifyMessageArrivedWorker.Stop();
                if (_notifyQueue != null && _notifyQueue.Count == 0)
                {
                    _notifyQueue.Add(null);
                }
            }
        }
        private void CheckBlockingPullRequest()
        {
            foreach (var entry in _queueRequestDict)
            {
                var items = entry.Key.Split(new string[] { Separator }, StringSplitOptions.None);
                var topic = items[0];
                var queueId = int.Parse(items[1]);
                var queueOffset = _queueService.GetQueueCurrentOffset(topic, queueId);
                NotifyMessageArrived(topic, queueId, queueOffset);
            }
        }
        private void CheckNewMessageExist(string key, string topic, int queueId, long queueOffset)
        {
            var currentQueueOffset = _queueService.GetQueueCurrentOffset(topic, queueId);
            if (currentQueueOffset > queueOffset)
            {
                PullRequest currentRequest;
                if (_queueRequestDict.TryRemove(key, out currentRequest))
                {
                    _logger.DebugFormat("New message arrived for PullRequest, current message queueOffset:{7}, PullRequest Id:{0}, RequestSequence:{6}, SuspendStartTime:{1}, ConsumerGroup:{2}, Topic:{3}, QueueId:{4}, QueueOffset:{5}",
                        currentRequest.Id,
                        currentRequest.SuspendStartTime,
                        currentRequest.PullMessageRequest.ConsumerGroup,
                        currentRequest.PullMessageRequest.MessageQueue.Topic,
                        currentRequest.PullMessageRequest.MessageQueue.QueueId,
                        currentRequest.PullMessageRequest.QueueOffset,
                        currentRequest.RemotingRequest.Sequence,
                        currentQueueOffset);
                    _taskFactory.StartNew(() => currentRequest.NewMessageArrivedAction(currentRequest));
                }
            }
        }
        private void NotifyMessageArrived(string topic, int queueId, long queueOffset)
        {
            var keyPrefix = BuildKeyPrefix(topic, queueId);
            var keys = _queueRequestDict.Keys.Where(x => x.StartsWith(keyPrefix));

            foreach (var key in keys)
            {
                PullRequest request;
                if (_queueRequestDict.TryGetValue(key, out request))
                {
                    if (queueOffset > request.PullMessageRequest.QueueOffset)
                    {
                        PullRequest currentRequest;
                        if (_queueRequestDict.TryRemove(key, out currentRequest))
                        {
                            _logger.DebugFormat("New message arrived for PullRequest, current message queueOffset:{7}, PullRequest Id:{0}, RequestSequence:{6}, SuspendStartTime:{1}, ConsumerGroup:{2}, Topic:{3}, QueueId:{4}, QueueOffset:{5}",
                                currentRequest.Id,
                                currentRequest.SuspendStartTime,
                                currentRequest.PullMessageRequest.ConsumerGroup,
                                currentRequest.PullMessageRequest.MessageQueue.Topic,
                                currentRequest.PullMessageRequest.MessageQueue.QueueId,
                                currentRequest.PullMessageRequest.QueueOffset,
                                currentRequest.RemotingRequest.Sequence,
                                queueOffset);

                            _taskFactory.StartNew(() => currentRequest.NewMessageArrivedAction(currentRequest));
                        }
                    }
                    else if (request.IsTimeout())
                    {
                        PullRequest currentRequest;
                        if (_queueRequestDict.TryRemove(key, out currentRequest))
                        {
                            _logger.DebugFormat("PullRequest timeout, PullRequest Id:{0}, RequestSequence:{6}, SuspendStartTime:{1}, ConsumerGroup:{2}, Topic:{3}, QueueId:{4}, QueueOffset:{5}",
                                currentRequest.Id,
                                currentRequest.SuspendStartTime,
                                currentRequest.PullMessageRequest.ConsumerGroup,
                                currentRequest.PullMessageRequest.MessageQueue.Topic,
                                currentRequest.PullMessageRequest.MessageQueue.QueueId,
                                currentRequest.PullMessageRequest.QueueOffset,
                                currentRequest.RemotingRequest.Sequence);

                            _taskFactory.StartNew(() => currentRequest.TimeoutAction(currentRequest));
                        }
                    }
                }
            }
        }
        private string BuildKeyPrefix(string topic, int queueId)
        {
            var builder = new StringBuilder();
            builder.Append(topic);
            builder.Append(Separator);
            builder.Append(queueId);
            builder.Append(Separator);
            return builder.ToString();
        }
        private string BuildKey(string topic, int queueId, string group)
        {
            var builder = new StringBuilder();
            builder.Append(topic);
            builder.Append(Separator);
            builder.Append(queueId);
            builder.Append(Separator);
            builder.Append(group);
            return builder.ToString();
        }

        class NotifyItem
        {
            public string Topic { get; set; }
            public int QueueId { get; set; }
            public long QueueOffset { get; set; }
        }
    }
}
