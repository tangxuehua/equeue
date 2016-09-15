using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.NameServer
{
    public class RouteInfoManager
    {
        #region Private Variables

        private readonly ConcurrentDictionary<string /*clusterName*/, Cluster> _routingInfoDict;
        private readonly object _lockObj = new object();
        private readonly IScheduleService _scheduleService;
        private readonly NameServerController _nameServerController;
        private readonly ILogger _logger;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IBinarySerializer _binarySerializer;

        #endregion

        public RouteInfoManager(NameServerController nameServerController)
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _routingInfoDict = new ConcurrentDictionary<string, Cluster>();
            _nameServerController = nameServerController;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _routingInfoDict.Clear();
            _scheduleService.StartTask("ScanNotActiveBroker", ScanNotActiveBroker, 1000, 1000);
        }
        public void Shutdown()
        {
            _routingInfoDict.Clear();
            _scheduleService.StopTask("ScanNotActiveBroker");
        }
        public void RegisterBroker(BrokerRegistrationRequest request)
        {
            lock (_lockObj)
            {
                var brokerInfo = request.BrokerInfo;
                var cluster = _routingInfoDict.GetOrAdd(brokerInfo.ClusterName, x => new Cluster { ClusterName = x });
                var brokerGroup = cluster.BrokerGroups.GetOrAdd(brokerInfo.GroupName, x => new BrokerGroup { GroupName = x });
                Broker broker;
                if (!brokerGroup.Brokers.TryGetValue(brokerInfo.BrokerName, out broker))
                {
                    broker = new Broker
                    {
                        BrokerInfo = request.BrokerInfo,
                        QueueInfoDict = request.QueueInfoDict,
                        LastActiveTime = DateTime.Now
                    };
                    if (brokerGroup.Brokers.TryAdd(brokerInfo.BrokerName, broker))
                    {
                        _logger.InfoFormat("Registered new broker, brokerInfo: {0}", _jsonSerializer.Serialize(brokerInfo));
                    }
                }
                else
                {
                    var newQueueInfo = _jsonSerializer.Serialize(request.QueueInfoDict);
                    var oldQueueInfo = _jsonSerializer.Serialize(broker.QueueInfoDict);

                    if (!broker.BrokerInfo.IsEqualsWith(request.BrokerInfo) || oldQueueInfo != newQueueInfo)
                    {
                        var logInfo = string.Format("Registered changed broker, newBrokerInfo: [BrokerInfo={0},QueueInfo={1}], oldBrokerInfo: [BrokerInfo={2},QueueInfo={3}]",
                            request.BrokerInfo,
                            newQueueInfo,
                            broker.BrokerInfo,
                            oldQueueInfo);
                        broker.BrokerInfo = request.BrokerInfo;
                        broker.QueueInfoDict = request.QueueInfoDict;
                        _logger.Info(logInfo);
                    }
                    broker.LastActiveTime = DateTime.Now;
                }
            }
        }
        public void UnregisterBroker(BrokerUnRegistrationRequest request)
        {
            lock (_lockObj)
            {
                var brokerInfo = request.BrokerInfo;
                var cluster = _routingInfoDict.GetOrAdd(brokerInfo.ClusterName, x => new Cluster { ClusterName = x });
                var brokerGroup = cluster.BrokerGroups.GetOrAdd(brokerInfo.GroupName, x => new BrokerGroup { GroupName = x });
                Broker broker;
                if (brokerGroup.Brokers.TryRemove(brokerInfo.BrokerName, out broker))
                {
                    _logger.InfoFormat("Unregistered broker, brokerInfo: {0}", _jsonSerializer.Serialize(broker));
                }
            }
        }
        public void UnregisterBroker(EndPoint brokerAddress)
        {
            lock (_lockObj)
            {
                foreach (var cluster in _routingInfoDict.Values)
                {
                    foreach (var brokerGroup in cluster.BrokerGroups.Values)
                    {
                        var toRemoveBroker = default(Broker);
                        foreach (var broker in brokerGroup.Brokers.Values)
                        {
                            if (broker.BrokerInfo.ProducerAddress == brokerAddress.ToAddress())
                            {
                                toRemoveBroker = broker;
                                break;
                            }
                        }
                        if (toRemoveBroker != null)
                        {
                            Broker removed;
                            if (brokerGroup.Brokers.TryRemove(toRemoveBroker.BrokerInfo.BrokerName, out removed))
                            {
                                _logger.InfoFormat("Removed closed broker, brokerInfo: {0}", _jsonSerializer.Serialize(removed.BrokerInfo));
                            }
                        }
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
                if (!_routingInfoDict.TryGetValue(request.ClusterName, out cluster))
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

                        var isTopicExist = false;
                        foreach (var entry in broker.QueueInfoDict)
                        {
                            var topic = entry.Key;
                            var queueInfo = entry.Value;

                            if (topic == request.Topic)
                            {
                                IList<int> queueList = null;
                                if (request.ClientRole == ClientRole.Producer)
                                {
                                    queueList = queueInfo.Where(x => x.ProducerVisible).Select(x => x.QueueId).ToList();
                                }
                                else if (request.ClientRole == ClientRole.Consumer)
                                {
                                    queueList = queueInfo.Where(x => x.ConsumerVisible).Select(x => x.QueueId).ToList();
                                }
                                else
                                {
                                    throw new Exception("Invalid clientRole: " + request.ClientRole);
                                }
                                var topicRouteInfo = new TopicRouteInfo
                                {
                                    BrokerInfo = broker.BrokerInfo,
                                    QueueInfo = queueList
                                };
                                returnList.Add(topicRouteInfo);
                                isTopicExist = true;
                                break;
                            }
                        }
                        if (!isTopicExist && _nameServerController.Setting.AutoCreateTopic)
                        {
                            var queueList = CreateTopicOnBroker(request.Topic, broker).ToList();
                            var topicRouteInfo = new TopicRouteInfo
                            {
                                BrokerInfo = broker.BrokerInfo,
                                QueueInfo = queueList
                            };
                            returnList.Add(topicRouteInfo);
                        }
                    }
                }

                return returnList;
            }
        }
        public IList<string> GetAllClusters()
        {
            return _routingInfoDict.Keys.ToList();
        }
        public IList<BrokerInfo> GetClusterBrokers(GetClusterBrokersRequest request)
        {
            lock (_lockObj)
            {
                var returnList = new List<BrokerInfo>();
                Cluster cluster;
                if (!_routingInfoDict.TryGetValue(request.ClusterName, out cluster))
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
                            if (broker.QueueInfoDict.Any(x => x.Key == request.Topic))
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

        private IEnumerable<int> CreateTopicOnBroker(string topic, Broker broker)
        {
            var brokerAdminEndpoint = broker.BrokerInfo.AdminAddress.ToEndPoint();
            var adminRemotingClient = new SocketRemotingClient(brokerAdminEndpoint, _nameServerController.Setting.SocketSetting).Start();
            var requestData = _binarySerializer.Serialize(new CreateTopicRequest(topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.CreateTopic, requestData);
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
                foreach (var cluster in _routingInfoDict.Values)
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
            public DateTime LastActiveTime { get; set; }
            public IDictionary<string /*topic*/, IList<QueueInfo>> QueueInfoDict = new Dictionary<string, IList<QueueInfo>>();

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
    }
}
