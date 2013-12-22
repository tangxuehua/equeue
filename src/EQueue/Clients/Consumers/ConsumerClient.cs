using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EQueue.Common;
using EQueue.Common.Extensions;
using EQueue.Common.IoC;
using EQueue.Common.Logging;
using EQueue.Common.Scheduling;

namespace EQueue.Clients.Consumers
{
    public class ConsumerClient
    {
        private readonly IDictionary<string, IConsumer> _consumerDict = new Dictionary<string, IConsumer>();
        private readonly IDictionary<string, TopicRouteData> _topicRouteDataDict = new Dictionary<string, TopicRouteData>();
        private readonly BlockingCollection<PullRequest> _pullRequestBlockingQueue = new BlockingCollection<PullRequest>(new ConcurrentQueue<PullRequest>());
        private readonly IScheduleService _scheduleService;
        private readonly Worker _executePullReqeustWorker;
        private readonly ILogger _logger;

        public string Id { get; private set; }
        public ConsumerSettings Settings { get; private set; }

        public ConsumerClient(ConsumerSettings settings, IScheduleService scheduleService) : this(string.Format("{0}@{1}", "Default", Utils.GetLocalIPV4()), settings, scheduleService)
        {
        }
        public ConsumerClient(string clientId, ConsumerSettings settings, IScheduleService scheduleService)
        {
            Id = clientId;
            Settings = settings;

            _executePullReqeustWorker = new Worker(ExecutePullRequest);
            _scheduleService = scheduleService;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public void Start()
        {
            StartScheduledTask();
            _executePullReqeustWorker.Start();

            _logger.InfoFormat("Consumer client [{0}] start OK, settings:{1}", Id, Settings);
        }

        public ConsumerClient RegisterConsumer(IConsumer consumer)
        {
            if (_consumerDict.ContainsKey(consumer.GroupName))
            {
                throw new Exception(string.Format("The consumer group[{0}] has been registered before, specify another name please.", consumer.GroupName));
            }
            _consumerDict[consumer.GroupName] = consumer;
            return this;
        }
        public void EnqueuePullRequest(PullRequest pullRequest)
        {
            _pullRequestBlockingQueue.Add(pullRequest);
        }
        public void EnqueuePullRequest(PullRequest pullRequest, int millisecondsDelay)
        {
            Task.Factory.StartDelayedTask(millisecondsDelay, () => _pullRequestBlockingQueue.Add(pullRequest));
        }

        public IEnumerable<string> FindConsumerClientIdList(string consumerGroup)
        {
            //TODO
            return null;
        }

        private void StartScheduledTask()
        {
            _scheduleService.ScheduleTask(Rebalance, 1000 * 10, 1000 * 10);
            _scheduleService.ScheduleTask(UpdateAllTopicRouteData, 1000 * 30, 1000 * 30);
            _scheduleService.ScheduleTask(SendHeartbeatToBroker, 1000 * 30, 1000 * 30);
            _scheduleService.ScheduleTask(PersistAllConsumerOffset, 1000 * 5, 1000 * 5);
        }

        #region Rebalance

        private void Rebalance()
        {
            foreach (var consumer in _consumerDict.Values)
            {
                try
                {
                    consumer.Rebalance();
                }
                catch (Exception ex)
                {
                    _logger.Error("Rebalance has exception.", ex);
                }
            }
        }

        #endregion

        #region Update topic route data

        private void UpdateAllTopicRouteData()
        {
            var topicList = new List<string>();
            foreach (var consumer in _consumerDict.Values)
            {
                topicList.AddRange(consumer.SubscriptionTopics);
            }
            foreach (var topic in topicList.Distinct())
            {
                UpdateTopicRouteData(topic);
            }
        }
        private void UpdateTopicRouteData(string topic)
        {
            var topicRouteDataFromServer = GetTopicRouteDataFromServer(topic);
            var topicRouteDataOnLocal = _topicRouteDataDict[topic];
            var changed = IsTopicRouteDataChanged(topicRouteDataOnLocal, topicRouteDataFromServer);

            if (!changed)
            {
                changed = IsNeedUpdateTopicRouteData(topic);
            }

            if (changed)
            {
                var consumeMessageQueues = new List<MessageQueue>();
                for (var index = 0; index < topicRouteDataFromServer.ConsumeQueueCount; index++)
                {
                    consumeMessageQueues.Add(new MessageQueue(topic, index));
                }

                foreach (var consumer in _consumerDict.Values)
                {
                    consumer.UpdateTopicSubscribeInfo(topic, consumeMessageQueues);
                }
                _topicRouteDataDict[topic] = topicRouteDataFromServer;
            }
        }
        private TopicRouteData GetTopicRouteDataFromServer(string topic)
        {
            //TODO
            return null;
        }
        private bool IsTopicRouteDataChanged(TopicRouteData oldData, TopicRouteData newData)
        {
            return oldData != newData;
        }
        private bool IsNeedUpdateTopicRouteData(string topic)
        {
            foreach (var consumer in _consumerDict.Values)
            {
                if (consumer.IsSubscribeTopicNeedUpdate(topic))
                {
                    return true;
                }
            }
            return false;
        }

        #endregion

        #region Send heartbeat to broker

        private void SendHeartbeatToBroker()
        {
            var heartbeatData = BuildHeartbeatData();

            if (heartbeatData.ConsumerDatas.Count() == 0)
            {
                _logger.Warn("Sending hearbeat, but no consumer.");
                return;
            }

            try
            {
                //TODO
                //this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                _logger.InfoFormat("Send heart beat to broker[{0}] success, heartbeatData:[{1}]", Settings.BrokerAddress, heartbeatData);
            }
            catch (Exception ex)
            {
                _logger.Error("Send heart beat to broker exception", ex);
            }
        }
        private HeartbeatData BuildHeartbeatData()
        {
            var consumerDataList = new List<ConsumerData>();
            foreach (var consumer in _consumerDict.Values)
            {
                consumerDataList.Add(new ConsumerData(consumer.GroupName, consumer.MessageModel, consumer.SubscriptionTopics));
            }

            return new HeartbeatData(Id, consumerDataList);
        }

        #endregion

        #region Persist all consumer offset

        private void PersistAllConsumerOffset()
        {
            foreach (var consumer in _consumerDict.Values)
            {
                consumer.PersistOffset();
            }
        }

        #endregion

        private void ExecutePullRequest()
        {
            var pullRequest = _pullRequestBlockingQueue.Take();
            try
            {
                IConsumer consumer;
                if (_consumerDict.TryGetValue(pullRequest.ConsumerGroup, out consumer))
                {
                    consumer.PullMessage(pullRequest);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("ExecutePullRequest exception. PullRequest: {0}.", pullRequest), ex);
            }
        }
    }
}
