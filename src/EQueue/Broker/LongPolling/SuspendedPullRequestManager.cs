using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using ECommon.IoC;
using ECommon.Scheduling;

namespace EQueue.Broker.LongPolling
{
    public class SuspendedPullRequestManager
    {
        private const string Topic_QueueId_Separator = "@";
        private ConcurrentDictionary<string, ConcurrentQueue<PullRequest>> _queueRequestDict = new ConcurrentDictionary<string, ConcurrentQueue<PullRequest>>();
        private readonly IScheduleService _scheduleService;
        private readonly IMessageService _messageService;
        private int _checkHoldRequestTaskId;

        public SuspendedPullRequestManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public void SuspendPullRequest(PullRequest pullRequest)
        {
            var key = BuildKey(pullRequest.PullMessageRequest.MessageQueue.Topic, pullRequest.PullMessageRequest.MessageQueue.QueueId);
            _queueRequestDict.AddOrUpdate(key,
            (x) =>
            {
                var queue = new ConcurrentQueue<PullRequest>();
                queue.Enqueue(pullRequest);
                return queue;
            },
            (x, queue) =>
            {
                queue.Enqueue(pullRequest);
                return queue;
            });
        }
        public void NotifyMessageArrived(string topic, int queueId, long queueOffset)
        {
            NotifyMessageArrived(BuildKey(topic, queueId), queueOffset);
        }

        public void Start()
        {
            _checkHoldRequestTaskId = _scheduleService.ScheduleTask(CheckHoldRequest, 1000, 1000);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_checkHoldRequestTaskId);
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
            ConcurrentQueue<PullRequest> queue;
            if (_queueRequestDict.TryGetValue(key, out queue))
            {
                var retryRequestList = new List<PullRequest>();
                PullRequest request;
                while (queue.TryDequeue(out request))
                {
                    if (queueOffset >= request.PullMessageRequest.QueueOffset)
                    {
                        request.NewMessageArrivedAction(request);
                    }
                    else if (DateTime.Now > request.SuspendTime.AddMilliseconds(request.SuspendMilliseconds))
                    {
                        request.SuspendTimeoutAction(request);
                    }
                    else
                    {
                        retryRequestList.Add(request);
                    }
                }
                if (retryRequestList.Count > 0)
                {
                    foreach (var retryRequest in retryRequestList)
                    {
                        queue.Enqueue(retryRequest);
                    }
                }
            }
        }
        private string BuildKey(string topic, int queueId)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(topic);
            builder.Append(Topic_QueueId_Separator);
            builder.Append(queueId);
            return builder.ToString();
        }
    }
}
