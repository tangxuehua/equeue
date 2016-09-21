using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Socketing;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Protocols.NameServers;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer
{
    public class ClusterManager
    {
        #region Private Variables

        private readonly ConcurrentDictionary<string /*clusterName*/, Cluster> _clusterDict;
        private readonly object _lockObj = new object();
        private readonly IScheduleService _scheduleService;
        private readonly NameServerController _nameServerController;
        private readonly ILogger _logger;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IBinarySerializer _binarySerializer;

        #endregion

        public ClusterManager(NameServerController nameServerController)
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _clusterDict = new ConcurrentDictionary<string, Cluster>();
            _nameServerController = nameServerController;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _clusterDict.Clear();
            _scheduleService.StartTask("ScanNotActiveBroker", ScanNotActiveBroker, 1000, 1000);
        }
        public void Shutdown()
        {
            _clusterDict.Clear();
            _scheduleService.StopTask("ScanNotActiveBroker");
        }
        public void RegisterBroker(ITcpConnection connection, BrokerRegistrationRequest request)
        {
            lock (_lockObj)
            {
                var brokerInfo = request.BrokerInfo;
                var cluster = _clusterDict.GetOrAdd(brokerInfo.ClusterName, x => new Cluster { ClusterName = x });
                var brokerGroup = cluster.BrokerGroups.GetOrAdd(brokerInfo.GroupName, x => new BrokerGroup { GroupName = x });
                Broker broker;
                if (!brokerGroup.Brokers.TryGetValue(brokerInfo.BrokerName, out broker))
                {
                    var connectionId = connection.RemotingEndPoint.ToAddress();
                    broker = new Broker
                    {
                        BrokerInfo = request.BrokerInfo,
                        TotalSendThroughput = request.TotalSendThroughput,
                        TotalConsumeThroughput = request.TotalConsumeThroughput,
                        TopicQueueInfoList = request.TopicQueueInfoList,
                        TopicConsumeInfoList = request.TopicConsumeInfoList,
                        ProducerList = request.ProducerList,
                        ConsumerList = request.ConsumerList,
                        Connection = connection,
                        ConnectionId = connectionId,
                        LastActiveTime = DateTime.Now,
                        Group = brokerGroup
                    };
                    if (brokerGroup.Brokers.TryAdd(brokerInfo.BrokerName, broker))
                    {
                        _logger.InfoFormat("Registered new broker, brokerInfo: {0}", _jsonSerializer.Serialize(brokerInfo));
                    }
                }
                else
                {
                    broker.LastActiveTime = DateTime.Now;
                    broker.TotalSendThroughput = request.TotalSendThroughput;
                    broker.TotalConsumeThroughput = request.TotalConsumeThroughput;

                    if (!broker.BrokerInfo.IsEqualsWith(request.BrokerInfo))
                    {
                        var logInfo = string.Format("Broker basicInfo changed, old: {0}, new: {1}", broker.BrokerInfo, request.BrokerInfo);
                        broker.BrokerInfo = request.BrokerInfo;
                        _logger.Info(logInfo);
                    }

                    broker.TopicQueueInfoList = request.TopicQueueInfoList;
                    broker.TopicConsumeInfoList = request.TopicConsumeInfoList;
                    broker.ProducerList = request.ProducerList;
                    broker.ConsumerList = request.ConsumerList;
                }
            }
        }
        public void UnregisterBroker(BrokerUnRegistrationRequest request)
        {
            lock (_lockObj)
            {
                var brokerInfo = request.BrokerInfo;
                var cluster = _clusterDict.GetOrAdd(brokerInfo.ClusterName, x => new Cluster { ClusterName = x });
                var brokerGroup = cluster.BrokerGroups.GetOrAdd(brokerInfo.GroupName, x => new BrokerGroup { GroupName = x });
                Broker removed;
                if (brokerGroup.Brokers.TryRemove(brokerInfo.BrokerName, out removed))
                {
                    _logger.InfoFormat("Unregistered broker, brokerInfo: {0}", _jsonSerializer.Serialize(removed.BrokerInfo));
                }
            }
        }
        public void RemoveBroker(ITcpConnection connection)
        {
            lock (_lockObj)
            {
                var connectionId = connection.RemotingEndPoint.ToAddress();
                var broker = FindBroker(connectionId);
                if (broker != null)
                {
                    Broker removed;
                    if (broker.Group.Brokers.TryRemove(broker.BrokerInfo.BrokerName, out removed))
                    {
                        _logger.InfoFormat("Removed broker, brokerInfo: {0}", _jsonSerializer.Serialize(removed.BrokerInfo));
                    }
                }
            }
        }
        public IList<TopicRouteInfo> GetTopicRouteInfo(GetTopicRouteInfoRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<TopicRouteInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return returnList;
                }

                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }

                        var queueList = new List<int>();
                        var topicQueueInfoList = broker.TopicQueueInfoList.Where(x => x.Topic == request.Topic).ToList();

                        if (topicQueueInfoList.Count > 0)
                        {
                            if (request.ClientRole == ClientRole.Producer)
                            {
                                queueList = topicQueueInfoList.Where(x => x.ProducerVisible).Select(x => x.QueueId).ToList();
                            }
                            else if (request.ClientRole == ClientRole.Consumer)
                            {
                                queueList = topicQueueInfoList.Where(x => x.ConsumerVisible).Select(x => x.QueueId).ToList();
                            }
                        }
                        else if (_nameServerController.Setting.AutoCreateTopic)
                        {
                            queueList = CreateTopicOnBroker(request.Topic, broker).ToList();
                        }

                        returnList.Add(new TopicRouteInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            QueueInfo = queueList
                        });
                    }
                }

                return returnList;
            }
        }
        public IList<BrokerTopicQueueInfo> GetTopicQueueInfo(Protocols.NameServers.Requests.GetTopicQueueInfoRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<BrokerTopicQueueInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return returnList;
                }

                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        var topicQueueInfoList = broker.TopicQueueInfoList.Where(x => x.Topic == request.Topic).ToList();
                        returnList.Add(new BrokerTopicQueueInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            TopicQueueInfoList = topicQueueInfoList
                        });
                    }
                }

                return returnList;
            }
        }
        public IList<BrokerTopicConsumeInfo> GetTopicConsumeInfo(Protocols.NameServers.Requests.GetTopicConsumeInfoRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<BrokerTopicConsumeInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return returnList;
                }

                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        var topicConsumeInfoList = broker.TopicConsumeInfoList.Where(x => x.Topic == request.Topic && x.ConsumerGroup == request.ConsumerGroup).ToList();
                        returnList.Add(new BrokerTopicConsumeInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            TopicConsumeInfoList = topicConsumeInfoList
                        });
                    }
                }

                return returnList;
            }
        }
        public IList<BrokerProducerListInfo> GetProducerList(GetProducerListRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<BrokerProducerListInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return returnList;
                }

                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        returnList.Add(new BrokerProducerListInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            ProducerList = broker.ProducerList
                        });
                    }
                }

                return returnList;
            }
        }
        public IList<BrokerConsumerListInfo> GetConsumerList(Protocols.NameServers.Requests.GetConsumerListRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<BrokerConsumerListInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return returnList;
                }

                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        var consumerList = broker.ConsumerList.Where(x => x.Topic == request.Topic && x.ConsumerGroup == request.ConsumerGroup).ToList();
                        returnList.Add(new BrokerConsumerListInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            ConsumerList = consumerList
                        });
                    }
                }

                return returnList;
            }
        }
        public IList<string> GetAllClusters()
        {
            return _clusterDict.Keys.ToList();
        }
        public IList<BrokerInfo> GetClusterBrokers(GetClusterBrokersRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<BrokerInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return returnList;
                }

                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        if (!string.IsNullOrEmpty(request.Topic))
                        {
                            if (broker.TopicQueueInfoList.Any(x => x.Topic == request.Topic))
                            {
                                returnList.Add(broker.BrokerInfo);
                            }
                        }
                        else
                        {
                            returnList.Add(broker.BrokerInfo);
                        }
                    }
                }

                returnList.Sort((x, y) => string.Compare(x.BrokerName, y.BrokerName));

                return returnList;
            }
        }
        public IList<BrokerStatusInfo> GetClusterBrokerStatusInfos(GetClusterBrokersRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<BrokerStatusInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return returnList;
                }

                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        if (!string.IsNullOrEmpty(request.Topic))
                        {
                            if (broker.TopicQueueInfoList.Any(x => x.Topic == request.Topic))
                            {
                                returnList.Add(new BrokerStatusInfo
                                {
                                    BrokerInfo = broker.BrokerInfo,
                                    TotalSendThroughput = broker.TotalSendThroughput,
                                    TotalConsumeThroughput = broker.TotalConsumeThroughput
                                });
                            }
                        }
                        else
                        {
                            returnList.Add(new BrokerStatusInfo
                            {
                                BrokerInfo = broker.BrokerInfo,
                                TotalSendThroughput = broker.TotalSendThroughput,
                                TotalConsumeThroughput = broker.TotalConsumeThroughput
                            });
                        }
                    }
                }

                returnList.Sort((x, y) => string.Compare(x.BrokerInfo.BrokerName, y.BrokerInfo.BrokerName));

                return returnList;
            }
        }

        private bool IsTopicQueueInfoChanged(IList<TopicQueueInfo> list1, IList<TopicQueueInfo> list2)
        {
            if (list1.Count != list2.Count)
            {
                return true;
            }
            for (var i = 0; i < list1.Count; i++)
            {
                var item1 = list1[i];
                var item2 = list2[i];

                if (item1.Topic != item2.Topic)
                {
                    return true;
                }
                if (item1.QueueId != item2.QueueId)
                {
                    return true;
                }
                if (item1.ProducerVisible != item2.ProducerVisible)
                {
                    return true;
                }
                if (item1.ConsumerVisible != item2.ConsumerVisible)
                {
                    return true;
                }
            }

            return false;
        }
        private bool IsTopicConsumeInfoChanged(IList<TopicConsumeInfo> list1, IList<TopicConsumeInfo> list2)
        {
            if (list1.Count != list2.Count)
            {
                return true;
            }
            for (var i = 0; i < list1.Count; i++)
            {
                var item1 = list1[i];
                var item2 = list2[i];

                if (item1.ConsumerGroup != item2.ConsumerGroup)
                {
                    return true;
                }
                if (item1.Topic != item2.Topic)
                {
                    return true;
                }
                if (item1.QueueId != item2.QueueId)
                {
                    return true;
                }
                if (item1.OnlineConsumerCount != item2.OnlineConsumerCount)
                {
                    return true;
                }
            }

            return false;
        }
        private bool IsConsumerInfoChanged(IList<ConsumerInfo> list1, IList<ConsumerInfo> list2)
        {
            if (list1.Count != list2.Count)
            {
                return true;
            }
            for (var i = 0; i < list1.Count; i++)
            {
                var item1 = list1[i];
                var item2 = list2[i];

                if (item1.ConsumerGroup != item2.ConsumerGroup)
                {
                    return true;
                }
                if (item1.Topic != item2.Topic)
                {
                    return true;
                }
                if (item1.QueueId != item2.QueueId)
                {
                    return true;
                }
                if (item1.ConsumerId != item2.ConsumerId)
                {
                    return true;
                }
            }

            return false;
        }
        private Broker FindBroker(string connectionId)
        {
            foreach (var cluster in _clusterDict.Values)
            {
                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (broker.ConnectionId == connectionId)
                        {
                            return broker;
                        }
                    }
                }
            }
            return null;
        }
        private IEnumerable<int> CreateTopicOnBroker(string topic, Broker broker)
        {
            var brokerAdminEndpoint = broker.BrokerInfo.AdminAddress.ToEndPoint();
            var adminRemotingClient = new SocketRemotingClient(brokerAdminEndpoint, _nameServerController.Setting.SocketSetting).Start();
            var requestData = _binarySerializer.Serialize(new CreateTopicRequest(topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.CreateTopic, requestData);
            var remotingResponse = adminRemotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("AutoCreateTopicOnBroker failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
            adminRemotingClient.Shutdown();
            return _binarySerializer.Deserialize<IEnumerable<int>>(remotingResponse.Body);
        }
        private void ScanNotActiveBroker()
        {
            lock (_lockObj)
            {
                foreach (var cluster in _clusterDict.Values)
                {
                    foreach (var brokerGroup in cluster.BrokerGroups.Values)
                    {
                        var notActiveBrokers = new List<Broker>();
                        foreach (var broker in brokerGroup.Brokers.Values)
                        {
                            if (broker.IsTimeout(_nameServerController.Setting.BrokerInactiveMaxMilliseconds))
                            {
                                notActiveBrokers.Add(broker);
                            }
                        }
                        if (notActiveBrokers.Count > 0)
                        {
                            foreach (var broker in notActiveBrokers)
                            {
                                Broker removed;
                                if (brokerGroup.Brokers.TryRemove(broker.BrokerInfo.BrokerName, out removed))
                                {
                                    _logger.InfoFormat("Removed timeout broker, brokerInfo: {0}, lastActiveTime: {1}", _jsonSerializer.Serialize(removed.BrokerInfo), removed.LastActiveTime);
                                }
                            }
                        }
                    }
                }
            }
        }

        class Broker
        {
            public BrokerInfo BrokerInfo { get; set; }
            public long TotalSendThroughput { get; set; }
            public long TotalConsumeThroughput { get; set; }
            public IList<TopicQueueInfo> TopicQueueInfoList = new List<TopicQueueInfo>();
            public IList<TopicConsumeInfo> TopicConsumeInfoList = new List<TopicConsumeInfo>();
            public IList<string> ProducerList = new List<string>();
            public IList<ConsumerInfo> ConsumerList = new List<ConsumerInfo>();
            public string ConnectionId { get; set; }
            public ITcpConnection Connection { get; set; }
            public DateTime LastActiveTime { get; set; }
            public BrokerGroup Group { get; set; }

            public bool IsTimeout(double timeoutMilliseconds)
            {
                return (DateTime.Now - LastActiveTime).TotalMilliseconds >= timeoutMilliseconds;
            }
        }
        class BrokerGroup
        {
            public string GroupName { get; set; }
            public ConcurrentDictionary<string /*brokerName*/, Broker> Brokers = new ConcurrentDictionary<string, Broker>();
        }
        class Cluster
        {
            public string ClusterName { get; set; }
            public ConcurrentDictionary<string /*groupName*/, BrokerGroup> BrokerGroups = new ConcurrentDictionary<string, BrokerGroup>();
        }
        class BrokerConnection
        {
            private readonly BrokerInfo _brokerInfo;
            private readonly SocketRemotingClient _adminRemotingClient;

            public BrokerInfo BrokerInfo
            {
                get { return _brokerInfo; }
            }
            public SocketRemotingClient AdminRemotingClient
            {
                get { return _adminRemotingClient; }
            }

            public BrokerConnection(BrokerInfo brokerInfo, SocketRemotingClient adminRemotingClient)
            {
                _brokerInfo = brokerInfo;
                _adminRemotingClient = adminRemotingClient;
            }

            public void Start()
            {
                _adminRemotingClient.Start();
            }
            public void Stop()
            {
                _adminRemotingClient.Shutdown();
            }
        }
    }
}
