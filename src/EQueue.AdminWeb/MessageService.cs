using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Protocols.NameServers;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.AdminWeb
{
    public class MessageService
    {
        class BrokerClient
        {
            public BrokerInfo BrokerInfo;
            public SocketRemotingClient RemotingClient;
        }
        private readonly object _lockObj = new object();
        private readonly byte[] EmptyBytes = new byte[0];
        private readonly IList<SocketRemotingClient> _nameServerRemotingClientList;
        private readonly ConcurrentDictionary<string /*clusterName*/, IList<BrokerClient>> _clusterBrokerDict;
        private readonly IBinarySerializer _binarySerializer;
        private long _nameServerIndex;

        public MessageService(IBinarySerializer binarySerializer)
        {
            _nameServerRemotingClientList = CreateRemotingClientList(Settings.NameServerList);
            _clusterBrokerDict = new ConcurrentDictionary<string, IList<BrokerClient>>();
            _binarySerializer = binarySerializer;
        }

        public void Start()
        {
            StartAllNameServerClients();
        }
        public IEnumerable<string> GetAllClusters()
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetAllClusters, EmptyBytes);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<string>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetAllClusters failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<BrokerStatusInfo> GetClusterBrokerStatusInfoList(string clusterName, string topic = null, bool onlyFindMaster = false)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new GetClusterBrokersRequest { ClusterName = clusterName, Topic = topic, OnlyFindMaster = onlyFindMaster });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetClusterBrokerStatusInfoList, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<BrokerStatusInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetClusterBrokers failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public BrokerStatisticInfo QueryBrokerStatisticInfo(string clusterName, string brokerName)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetBrokerStatisticInfo, new byte[0]);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<BrokerStatisticInfo>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("QueryBrokerStatisticInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<BrokerTopicQueueInfo> GetTopicQueueInfoList(string clusterName, string topic)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new Protocols.NameServers.Requests.GetTopicQueueInfoRequest
            {
                ClusterName = clusterName,
                Topic = topic
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetTopicQueueInfo, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<BrokerTopicQueueInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<TopicQueueInfo> GetTopicQueueInfoList(string clusterName, string brokerName, string topic)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new Protocols.Brokers.Requests.GetTopicQueueInfoRequest(topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetTopicQueueInfo, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<TopicQueueInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<BrokerTopicConsumeInfo> GetTopicConsumeInfoList(string clusterName, string group, string topic)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new Protocols.NameServers.Requests.GetTopicConsumeInfoRequest
            {
                ClusterName = clusterName,
                ConsumerGroup = group,
                Topic = topic
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetTopicConsumeInfo, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<BrokerTopicConsumeInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetTopicConsumeInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<TopicConsumeInfo> GetTopicConsumeInfoList(string clusterName, string brokerName, string group, string topic)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new Protocols.Brokers.Requests.GetTopicConsumeInfoRequest(group, topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetTopicConsumeInfo, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<TopicConsumeInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetTopicConsumeInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<BrokerProducerListInfo> GetProducerInfoList(string clusterName)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new GetProducerListRequest
            {
                ClusterName = clusterName
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetProducerList, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<BrokerProducerListInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetProducerInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<string> GetProducerInfoList(string clusterName, string brokerName)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetProducerList, EmptyBytes);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                var producerIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return producerIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                throw new Exception(string.Format("GetProducerInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<BrokerConsumerListInfo> GetConsumerInfoList(string clusterName, string group, string topic)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new Protocols.NameServers.Requests.GetConsumerListRequest
            {
                ClusterName = clusterName,
                ConsumerGroup = group,
                Topic = topic
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetConsumerList, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<BrokerConsumerListInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetConsumerInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<ConsumerInfo> GetConsumerInfoList(string clusterName, string brokerName, string group, string topic)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new Protocols.Brokers.Requests.GetConsumerListRequest(group, topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetConsumerList, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<ConsumerInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetConsumerInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<string> GetLatestSendMessagesList(string clusterName, string brokerName)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetLastestMessages, EmptyBytes);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == ResponseCode.Success)
            {
                var messageIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return messageIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                throw new Exception(string.Format("GetLatestSendMessagesList failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void CreateTopic(string clusterName, string topic, int? initialQueueCount)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new CreateTopicForClusterRequest
            {
                ClusterName = clusterName,
                Topic = topic,
                InitialQueueCount = initialQueueCount
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.CreateTopic, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("CreateTopic failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void CreateTopic(string clusterName, string brokerName, string topic, int? initialQueueCount)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new CreateTopicRequest(topic, initialQueueCount));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.CreateTopic, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("CreateTopic failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteTopic(string clusterName, string topic)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new DeleteTopicForClusterRequest
            {
                ClusterName = clusterName,
                Topic = topic
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.DeleteTopic, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteTopic failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteTopic(string clusterName, string brokerName, string topic)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new DeleteTopicRequest(topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.DeleteTopic, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteTopic failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void AddQueue(string clusterName, string topic)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new AddQueueForClusterRequest
            {
                ClusterName = clusterName,
                Topic = topic
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.AddQueue, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("AddQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void AddQueue(string clusterName, string brokerName, string topic)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new AddQueueRequest(topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.AddQueue, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("AddQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteQueue(string clusterName, string topic, int queueId)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new DeleteQueueForClusterRequest
            {
                ClusterName = clusterName,
                Topic = topic,
                QueueId = queueId
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.DeleteQueue, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteQueue(string clusterName, string brokerName, string topic, int queueId)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new DeleteQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.DeleteQueue, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueProducerVisible(string clusterName, string topic, int queueId, bool visible)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new SetQueueProducerVisibleForClusterRequest
            {
                ClusterName = clusterName,
                Topic = topic,
                QueueId = queueId,
                Visible = visible
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.SetQueueProducerVisible, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueProducerVisible failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueProducerVisible(string clusterName, string brokerName, string topic, int queueId, bool visible)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new SetQueueProducerVisibleRequest(topic, queueId, visible));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.SetQueueProducerVisible, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueProducerVisible failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueConsumerVisible(string clusterName, string topic, int queueId, bool visible)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new SetQueueConsumerVisibleForClusterRequest
            {
                ClusterName = clusterName,
                Topic = topic,
                QueueId = queueId,
                Visible = visible
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.SetQueueConsumerVisible, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueConsumerVisible failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueConsumerVisible(string clusterName, string brokerName, string topic, int queueId, bool visible)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            var requestData = _binarySerializer.Serialize(new SetQueueConsumerVisibleRequest(topic, queueId, visible));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.SetQueueConsumerVisible, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueConsumerVisible failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueNextConsumeOffset(string clusterName, string consumerGroup, string topic, int queueId, long nextOffset)
        {
            if (nextOffset < 0)
            {
                throw new ArgumentException("nextOffset cannot be small than zero.");
            }
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new SetQueueNextConsumeOffsetForClusterRequest
            {
                ClusterName = clusterName,
                ConsumerGroup = consumerGroup,
                Topic = topic,
                QueueId = queueId,
                NextOffset = nextOffset
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.SetQueueNextConsumeOffset, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueNextConsumeOffset failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueNextConsumeOffset(string clusterName, string brokerName, string consumerGroup, string topic, int queueId, long nextOffset)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            if (nextOffset < 0)
            {
                throw new ArgumentException("nextOffset cannot be small than zero.");
            }
            var requestData = _binarySerializer.Serialize(new SetQueueNextConsumeOffsetRequest(consumerGroup, topic, queueId, nextOffset));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.SetQueueNextConsumeOffset, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueNextConsumeOffset failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteConsumerGroup(string clusterName, string consumerGroup)
        {
            if (string.IsNullOrEmpty(consumerGroup))
            {
                throw new ArgumentException("consumerGroup cannot be null or empty.");
            }
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new DeleteConsumerGroupForClusterRequest
            {
                ClusterName = clusterName,
                GroupName = consumerGroup
            });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.DeleteConsumerGroup, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteConsumerGroup failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteConsumerGroup(string clusterName, string brokerName, string consumerGroup)
        {
            var remotingClient = GetBrokerByName(clusterName, brokerName);
            if (string.IsNullOrEmpty(consumerGroup))
            {
                throw new ArgumentException("consumerGroup cannot be null or empty.");
            }
            var requestData = _binarySerializer.Serialize(new DeleteConsumerGroupRequest(consumerGroup));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.DeleteConsumerGroup, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteConsumerGroup failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public QueueMessage GetMessageDetail(string clusterName, string messageId)
        {
            var brokerClientList = GetClusterBrokers(clusterName);

            foreach (var brokerClient in brokerClientList)
            {
                var requestData = _binarySerializer.Serialize(new GetMessageDetailRequest(messageId));
                var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetMessageDetail, requestData);
                var remotingResponse = brokerClient.RemotingClient.InvokeSync(remotingRequest, 30000);
                if (remotingResponse.Code == ResponseCode.Success)
                {
                    return _binarySerializer.Deserialize<IEnumerable<QueueMessage>>(remotingResponse.Body).SingleOrDefault();
                }
                else
                {
                    throw new Exception(string.Format("GetMessageDetail failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
                }
            }

            return null;
        }

        private void StartAllNameServerClients()
        {
            foreach (var nameServerRemotingClient in _nameServerRemotingClientList)
            {
                nameServerRemotingClient.Start();
            }
        }
        private SocketRemotingClient GetAvailableNameServerRemotingClient()
        {
            var availableList = _nameServerRemotingClientList.Where(x => x.IsConnected).ToList();
            if (availableList.Count == 0)
            {
                throw new Exception("No available name server.");
            }
            return availableList[(int)(Interlocked.Increment(ref _nameServerIndex) % availableList.Count)];
        }
        private void RefreshClusterBrokers(string clusterName)
        {
            var remotingClient = GetAvailableNameServerRemotingClient();
            var requestData = _binarySerializer.Serialize(new GetClusterBrokersRequest { ClusterName = clusterName });
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetClusterBrokers, requestData);
            var remotingResponse = remotingClient.InvokeSync(remotingRequest, 30000);

            if (remotingResponse.Code == ResponseCode.Success)
            {
                var brokerInfoList = _binarySerializer.Deserialize<IEnumerable<BrokerInfo>>(remotingResponse.Body);
                var brokerClientList = new List<BrokerClient>();
                foreach (var brokerInfo in brokerInfoList)
                {
                    var client = new SocketRemotingClient(brokerInfo.AdminAddress.ToEndPoint(), Settings.SocketSetting).Start();
                    var brokerClient = new BrokerClient { BrokerInfo = brokerInfo, RemotingClient = client };
                    brokerClientList.Add(brokerClient);
                }
                IList<BrokerClient> removedList;
                if (_clusterBrokerDict.TryRemove(clusterName, out removedList))
                {
                    foreach (var brokerClient in removedList)
                    {
                        brokerClient.RemotingClient.Shutdown();
                    }
                }
                _clusterBrokerDict.TryAdd(clusterName, brokerClientList);
            }
            else
            {
                throw new Exception(string.Format("GetClusterBrokers failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        private SocketRemotingClient GetBrokerByName(string clusterName, string brokerName)
        {
            IList<BrokerClient> clientList;
            if (!_clusterBrokerDict.TryGetValue(clusterName, out clientList))
            {
                RefreshClusterBrokers(clusterName);
                if (!_clusterBrokerDict.TryGetValue(clusterName, out clientList))
                {
                    return null;
                }
            }
            var brokerClient = clientList.SingleOrDefault(x => x.BrokerInfo.BrokerName == brokerName);
            if (brokerClient == null)
            {
                RefreshClusterBrokers(clusterName);
                if (!_clusterBrokerDict.TryGetValue(clusterName, out clientList))
                {
                    return null;
                }
                brokerClient = clientList.SingleOrDefault(x => x.BrokerInfo.BrokerName == brokerName);
            }
            if (brokerClient != null)
            {
                return brokerClient.RemotingClient;
            }
            return null;
        }
        private IList<BrokerClient> GetClusterBrokers(string clusterName)
        {
            IList<BrokerClient> clientList;
            if (!_clusterBrokerDict.TryGetValue(clusterName, out clientList))
            {
                RefreshClusterBrokers(clusterName);
                if (!_clusterBrokerDict.TryGetValue(clusterName, out clientList))
                {
                    return null;
                }
            }
            return clientList;
        }
        private IList<SocketRemotingClient> CreateRemotingClientList(IEnumerable<IPEndPoint> endpointList)
        {
            var remotingClientList = new List<SocketRemotingClient>();
            foreach (var endpoint in endpointList)
            {
                var remotingClient = new SocketRemotingClient(endpoint, Settings.SocketSetting);
                remotingClientList.Add(remotingClient);
            }
            return remotingClientList;
        }
    }
}
