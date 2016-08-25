using System.Collections.Concurrent;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.NameServer
{
    public class RouteInfoManager
    {
        private readonly ConcurrentDictionary<string /*brokerName*/, ConcurrentDictionary<string /*brokerAddress*/, int /*brokerRole*/>> _brokerInfoDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, int>>();
        private readonly ConcurrentDictionary<string /*topic*/, ConcurrentDictionary<string /*brokerName*/, IList<int> /*queueId list*/>> _topicRouteInfoDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, IList<int>>>();
        private readonly object _lockObj = new object();

        public void RegisterController(string brokerName, string brokerAddress, BrokerRole brokerRole, IDictionary<string, int> queueDict)
        {
            lock (_lockObj)
            {
                var brokerDict = _brokerInfoDict.GetOrAdd(brokerName, x => new ConcurrentDictionary<string, int>());
                brokerDict.TryAdd(brokerAddress, (int)brokerRole);

                foreach (var entry in queueDict)
                {
                    var topic = entry.Key;
                    var queueId = entry.Value;
                    var routeInfoDict = _topicRouteInfoDict.GetOrAdd(topic, x => new ConcurrentDictionary<string, IList<int>>());
                    var queueIdList = routeInfoDict.GetOrAdd(brokerName, x => new List<int>());
                    queueIdList.Add(queueId);
                }
            }
        }
        public void UnregisterController(string brokerName, string brokerAddress)
        {
            lock (_lockObj)
            {
                ConcurrentDictionary<string, int> brokerDict;
                if (_brokerInfoDict.TryGetValue(brokerName, out brokerDict))
                {
                    int role;
                    brokerDict.TryRemove(brokerAddress, out role);
                }
            }
        }
        public IList<TopicRouteInfo> GetTopicRouteInfo(string topic)
        {
            lock (_lockObj)
            {
                var returnList = new List<TopicRouteInfo>();
                var brokerQueueInfo = _topicRouteInfoDict.GetOrAdd(topic, x => new ConcurrentDictionary<string, IList<int>>());

                foreach (var brokerQueue in brokerQueueInfo)
                {
                    var brokerName = brokerQueue.Key;
                    var queueIds = brokerQueue.Value;
                    if (queueIds.Count == 0)
                    {
                        continue;
                    }
                    foreach (var queueId in queueIds)
                    {
                        ConcurrentDictionary<string, int> brokerAddressDict;
                        if (!_brokerInfoDict.TryGetValue(brokerName, out brokerAddressDict) || brokerAddressDict.Count == 0)
                        {
                            break;
                        }

                        var returnItem = new TopicRouteInfo();
                        var brokerInfo = new BrokerInfo();
                        returnItem.Topic = topic;
                        returnItem.QueueId = queueId;
                        returnItem.BrokerInfo = brokerInfo;
                        brokerInfo.BrokerName = brokerName;
                        foreach (var brokerAddress in brokerAddressDict)
                        {
                            brokerInfo.BrokerAddresses.Add(brokerAddress.Key, brokerAddress.Value);
                        }
                        returnList.Add(returnItem);
                    }
                }

                return returnList;
            }
        }
    }
}
