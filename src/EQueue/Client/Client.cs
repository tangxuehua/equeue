using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EQueue.Common;
using EQueue.Common.Extensions;
using EQueue.Common.Logging;

namespace EQueue
{
    public class Client
    {
        private readonly ConcurrentDictionary<string, IProducer> _producerDict = new ConcurrentDictionary<string, IProducer>();
        private readonly ConcurrentDictionary<string, IConsumer> _consumerDict = new ConcurrentDictionary<string, IConsumer>();
        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteDataDict = new ConcurrentDictionary<string, TopicRouteData>();
        private readonly BlockingCollection<PullRequest> _pullRequestBlockingQueue = new BlockingCollection<PullRequest>(new ConcurrentQueue<PullRequest>());
        private readonly ILogger _logger;
        private readonly ClientConfig _config;
        private readonly IScheduleService _scheduleService;
        private readonly Worker _fetchPullReqeustWorker;

        public string ClientId { get; private set; }

        public Client(
            string clientId,
            ClientConfig config,
            IScheduleService scheduleService,
            ILoggerFactory loggerFactory)
        {
            ClientId = clientId;
            _config = config;
            _fetchPullReqeustWorker = new Worker(FetchAndExecutePullRequest);
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create(GetType().Name);
        }

        public void Start()
        {
            StartScheduledTask();
            _fetchPullReqeustWorker.Start();

            _logger.InfoFormat("client [{0}] start OK, Config:{1}", ClientId, _config);
        }

        public void EnqueuePullRequest(PullRequest pullRequest)
        {
            _pullRequestBlockingQueue.Add(pullRequest);
        }
        public void EnqueuePullRequest(PullRequest pullRequest, int millisecondsDelay)
        {
            Task.Factory.StartDelayedTask(millisecondsDelay, () => _pullRequestBlockingQueue.Add(pullRequest));
        }

        public IEnumerable<string> FindConsumerIdList(string consumerGroup)
        {
            //TODO
            return null;
        }

        private void StartScheduledTask()
        {
            _scheduleService.ScheduleTask(Rebalance, 1000 * 10, 1000 * 10);
            _scheduleService.ScheduleTask(UpdateTopicRouteInfoFromNameServer, 1000 * 30, 1000 * 30);
            _scheduleService.ScheduleTask(SendHeartbeatToBroker, 1000 * 30, 1000 * 30);
            _scheduleService.ScheduleTask(PersistAllConsumerOffset, 1000 * 5, 1000 * 5);
        }

        private void Rebalance()
        {
            foreach (var consumer in _consumerDict.Values)
            {
                try
                {
                    consumer.DoRebalance();
                }
                catch (Exception ex)
                {
                    _logger.Error("Rebalance has exception.", ex);
                }
            }
        }
        private void UpdateTopicRouteInfoFromNameServer()
        {

        }
        private void SendHeartbeatToBroker()
        {
            var heartbeatData = BuildHeartbeatData();

            if (heartbeatData.ProducerDatas.Count() == 0 && heartbeatData.ConsumerDatas.Count() == 0)
            {
                _logger.Warn("sending hearbeat, but no consumer and no producer");
                return;
            }

            try
            {
                //this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                _logger.InfoFormat("send heart beat to broker[{0}] success, heartbeatData:[{1}]", _config.BrokerAddress, heartbeatData);
            }
            catch (Exception ex)
            {
                _logger.Error("send heart beat to broker exception", ex);
            }
        }
        private HeartbeatData BuildHeartbeatData()
        {
            var producerDataList = new List<ProducerData>();
            var consumerDataList = new List<ConsumerData>();

            foreach (var producerGroup in _producerDict.Keys)
            {
                producerDataList.Add(new ProducerData(producerGroup));
            }
            foreach (var consumer in _consumerDict.Values)
            {
                consumerDataList.Add(new ConsumerData(consumer.GroupName, consumer.MessageModel, consumer.SubscriptionTopics));
            }

            return new HeartbeatData(ClientId, producerDataList, consumerDataList);
        }
        private void PersistAllConsumerOffset()
        {
            foreach (var consumer in _consumerDict.Values)
            {
                try
                {
                    consumer.PersistOffset();
                }
                catch (Exception ex)
                {
                    _logger.Error("PersistConsumerOffset has exception.", ex);
                }
            }
        }
        private void FetchAndExecutePullRequest()
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
                _logger.Error(string.Format("pull message exception. PullRequest: {0}.", pullRequest), ex);
                //TODO, to check if we need this code here.
                //_pullRequestQueue.Add(pullRequest);
            }
        }
    }
}
