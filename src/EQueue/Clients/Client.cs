using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;
using EQueue.Common;
using EQueue.Common.Extensions;
using EQueue.Common.IoC;
using EQueue.Common.Logging;
using EQueue.Common.Scheduling;

namespace EQueue.Clients
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

        public Client(string clientId, ClientConfig config, IScheduleService scheduleService)
        {
            ClientId = clientId;
            _config = config;
            _fetchPullReqeustWorker = new Worker(FetchAndExecutePullRequest);
            _scheduleService = scheduleService;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public void Start()
        {
            StartScheduledTask();
            _fetchPullReqeustWorker.Start();

            _logger.InfoFormat("client [{0}] start OK, Config:{1}", ClientId, _config);
        }

        public void RegisterProducer(IProducer producer)
        {
            if (_producerDict.ContainsKey(producer.GroupName))
            {
                throw new Exception(string.Format("The producer group[{0}] has been registered before, specify another name please.", producer.GroupName));
            }
            _producerDict[producer.GroupName] = producer;
        }
        public void RegisterConsumer(IConsumer consumer)
        {
            if (_consumerDict.ContainsKey(consumer.GroupName))
            {
                throw new Exception(string.Format("The consumer group[{0}] has been registered before, specify another name please.", consumer.GroupName));
            }
            _consumerDict[consumer.GroupName] = consumer;
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
            var topicList = new List<string>();
            foreach (var consumer in _consumerDict.Values)
            {
                topicList.AddRange(consumer.SubscriptionTopics);
            }
            foreach (var producer in _producerDict.Values)
            {
                topicList.AddRange(producer.PublishTopics);
            }
            var distinctTopics = topicList.Distinct();

            foreach (var topic in distinctTopics)
            {
                UpdateTopicRouteInfoFromNameServer(topic);
            }

        }
        private void UpdateTopicRouteInfoFromNameServer(string topic)
        {
            var topicRouteData = GetTopicRouteInfoFromNameServer(topic);
            var oldTopicRouteData = _topicRouteDataDict[topic];
            var changed = IsTopicRouteDataChanged(oldTopicRouteData, topicRouteData);

            if (!changed)
            {
                changed = IsNeedUpdateTopicRouteInfo(topic);
            }

            if (changed)
            {
                var publishMessageQueues = new List<MessageQueue>();
                var consumeMessageQueues = new List<MessageQueue>();
                for (var index = 0; index < topicRouteData.PublishQueueCount; index++)
                {
                    publishMessageQueues.Add(new MessageQueue(topic, _config.BrokerAddress, index));
                }

                for (var index = 0; index < topicRouteData.ConsumeQueueCount; index++)
                {
                    consumeMessageQueues.Add(new MessageQueue(topic, _config.BrokerAddress, index));
                }

                foreach (var producer in _producerDict.Values)
                {
                    producer.UpdateTopicPublishInfo(topic, publishMessageQueues);
                }
                foreach (var consumer in _consumerDict.Values)
                {
                    consumer.UpdateTopicSubscribeInfo(topic, consumeMessageQueues);
                }
                _topicRouteDataDict[topic] = topicRouteData;
            }
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
                //TODO
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
                consumer.PersistOffset();
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
                _logger.Error(string.Format("FetchAndExecutePullRequest exception. PullRequest: {0}.", pullRequest), ex);
            }
        }
        private TopicRouteData GetTopicRouteInfoFromNameServer(string topic)
        {
            //TODO
            return null;
        }
        private bool IsTopicRouteDataChanged(TopicRouteData oldData, TopicRouteData newData)
        {
            return oldData != newData;
        }
        private bool IsNeedUpdateTopicRouteInfo(string topic)
        {
            foreach (var producer in _producerDict.Values)
            {
                if (producer.IsPublishTopicNeedUpdate(topic))
                {
                    return true;
                }
            }
            foreach (var consumer in _consumerDict.Values)
            {
                if (consumer.IsSubscribeTopicNeedUpdate(topic))
                {
                    return true;
                }
            }
            return false;
        }
    }
}
