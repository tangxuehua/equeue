using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using ECommon.IoC;
using ECommon.Scheduling;

namespace EQueue.Broker.LongPolling
{
    public class SuspendedPullRequestManager
    {
        private const string Topic_QueueId_Separator = "@";
        private readonly ConcurrentDictionary<string, PullRequest> _queueRequestDict = new ConcurrentDictionary<string, PullRequest>();
        private readonly IScheduleService _scheduleService;
        private readonly IMessageService _messageService;
        private readonly BlockingCollection<NotifyItem> _notifyQueue = new BlockingCollection<NotifyItem>(new ConcurrentQueue<NotifyItem>());
        private readonly Worker _worker;
        private int _checkHoldRequestTaskId;

        public SuspendedPullRequestManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _worker = new Worker(() =>
            {
                var notifyItem = _notifyQueue.Take();
                NotifyMessageArrived(BuildKey(notifyItem.Topic, notifyItem.QueueId), notifyItem.QueueOffset);
            });
        }

        public void SuspendPullRequest(PullRequest pullRequest)
        {
            var key = BuildKey(pullRequest.PullMessageRequest.MessageQueue.Topic, pullRequest.PullMessageRequest.MessageQueue.QueueId);
            var changed = false;
            var existingRequest = default(PullRequest);
            _queueRequestDict.AddOrUpdate(key, pullRequest, (x, request) =>
            {
                existingRequest = request;
                changed = true;
                return pullRequest;
            });
            if (changed && existingRequest != null)
            {
                var currentRequest = existingRequest;
                Task.Factory.StartNew(() => currentRequest.ReplacedAction(currentRequest));
            }
        }
        public void NotifyMessageArrived(string topic, int queueId, long queueOffset)
        {
            _notifyQueue.Add(new NotifyItem { Topic = topic, QueueId = queueId, QueueOffset = queueOffset });
        }

        public void Start()
        {
            _checkHoldRequestTaskId = _scheduleService.ScheduleTask(CheckHoldRequest, 1000, 1000);
            _worker.Start();
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_checkHoldRequestTaskId);
            _worker.Stop();
        }

        private void CheckHoldRequest()
        {
            foreach (var entry in _queueRequestDict)
            {
                var items = entry.Key.Split(new string[] { Topic_QueueId_Separator }, StringSplitOptions.None);
                var topic = items[0];
                var queueId = int.Parse(items[1]);
                var queueOffset = _messageService.GetQueueCurrentOffset(topic, queueId);
                NotifyMessageArrived(entry.Key, queueOffset);
            }
        }
        private void NotifyMessageArrived(string key, long queueOffset)
        {
            PullRequest request;
            if (_queueRequestDict.TryGetValue(key, out request))
            {
                if (queueOffset >= request.PullMessageRequest.QueueOffset)
                {
                    PullRequest currentRequest;
                    if (_queueRequestDict.TryRemove(key, out currentRequest))
                    {
                        Task.Factory.StartNew(() => currentRequest.NewMessageArrivedAction(currentRequest));
                    }
                }
                else if (request.IsTimeout())
                {
                    PullRequest currentRequest;
                    if (_queueRequestDict.TryRemove(key, out currentRequest))
                    {
                        Task.Factory.StartNew(() => currentRequest.SuspendTimeoutAction(currentRequest));
                    }
                }
            }
        }
        private string BuildKey(string topic, int queueId)
        {
            var builder = new StringBuilder();
            builder.Append(topic);
            builder.Append(Topic_QueueId_Separator);
            builder.Append(queueId);
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
