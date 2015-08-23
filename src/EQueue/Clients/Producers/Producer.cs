using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Socketing;
using ECommon.Utilities;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Clients.Producers
{
    public class Producer
    {
        private readonly object _lockObject;
        private readonly ConcurrentDictionary<string, IList<int>> _topicQueueIdsDict;
        private readonly IScheduleService _scheduleService;
        private readonly SocketRemotingClient _remotingClient;
        private readonly IQueueSelector _queueSelector;
        private readonly ILogger _logger;

        public string Id { get; private set; }
        public ProducerSetting Setting { get; private set; }

        public Producer(string id) : this(id, null) { }
        public Producer(string id, ProducerSetting setting)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }
            Id = id;
            Setting = setting ?? new ProducerSetting();

            _lockObject = new object();
            _topicQueueIdsDict = new ConcurrentDictionary<string, IList<int>>();
            _remotingClient = new SocketRemotingClient(Setting.BrokerAddress, Setting.LocalAddress);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _queueSelector = ObjectContainer.Resolve<IQueueSelector>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _remotingClient.RegisterConnectionEventListener(new ConnectionEventListener(this));
        }

        public Producer RegisterResponseHandler(IResponseHandler responseHandler)
        {
            _remotingClient.RegisterResponseHandler((int)RequestCode.SendMessage, responseHandler);
            return this;
        }
        public Producer Start()
        {
            _remotingClient.Start();
            _logger.InfoFormat("Started, producerId:{0}", Id);
            return this;
        }
        public Producer Shutdown()
        {
            _remotingClient.Shutdown();
            _logger.InfoFormat("Shutdown, producerId:{0}", Id);
            return this;
        }
        public SendResult Send(Message message, string routingKey, int timeoutMilliseconds = 30000)
        {
            var sendResult = SendAsync(message, routingKey, timeoutMilliseconds).WaitResult<SendResult>(timeoutMilliseconds + 1000);
            if (sendResult == null)
            {
                sendResult = new SendResult(SendStatus.Timeout, null, string.Format("Send message timeout, message: {0}, routingKey: {1}, timeoutMilliseconds: {2}", message, routingKey, timeoutMilliseconds));
            }
            return sendResult;
        }
        public async Task<SendResult> SendAsync(Message message, string routingKey, int timeoutMilliseconds = 30000)
        {
            Ensure.NotNull(message, "message");

            var queueId = GetAvailableQueueId(message, routingKey);
            if (queueId < 0)
            {
                throw new Exception(string.Format("No available routing queue for topic [{0}].", message.Topic));
            }
            var remotingRequest = BuildSendMessageRequest(message, queueId, routingKey);

            try
            {
                var remotingResponse = await _remotingClient.InvokeAsync(remotingRequest, timeoutMilliseconds).ConfigureAwait(false);

                if (remotingResponse == null)
                {
                    return new SendResult(SendStatus.Timeout, null, string.Format("Send message timeout, message: {0}, routingKey: {1}, timeoutMilliseconds: {2}", message, routingKey, timeoutMilliseconds));
                }

                return ParseSendResult(remotingResponse);
            }
            catch (Exception ex)
            {
                return new SendResult(SendStatus.Failed, null, ex.Message);
            }
        }
        public void SendWithCallback(Message message, string routingKey)
        {
            Ensure.NotNull(message, "message");

            var queueId = GetAvailableQueueId(message, routingKey);
            if (queueId < 0)
            {
                throw new Exception(string.Format("No available routing queue for topic [{0}].", message.Topic));
            }
            var remotingRequest = BuildSendMessageRequest(message, queueId, routingKey);

            _remotingClient.InvokeWithCallback(remotingRequest);
        }
        public void SendOneway(Message message, string routingKey)
        {
            Ensure.NotNull(message, "message");

            var queueId = GetAvailableQueueId(message, routingKey);
            if (queueId < 0)
            {
                throw new Exception(string.Format("No available routing queue for topic [{0}].", message.Topic));
            }
            var remotingRequest = BuildSendMessageRequest(message, queueId, routingKey);

            _remotingClient.InvokeOneway(remotingRequest);
        }

        public static SendResult ParseSendResult(RemotingResponse remotingResponse)
        {
            if (remotingResponse.Code == ResponseCode.Success)
            {
                var messageResult = MessageUtils.DecodeMessageSendResponse(remotingResponse.Body);
                return new SendResult(SendStatus.Success, messageResult, null);
            }
            else if (remotingResponse.Code == 0)
            {
                return new SendResult(SendStatus.Timeout, null, Encoding.UTF8.GetString(remotingResponse.Body));
            }
            else
            {
                return new SendResult(SendStatus.Failed, null, Encoding.UTF8.GetString(remotingResponse.Body));
            }
        }

        private IList<int> GetTopicQueueIds(string topic)
        {
            IList<int> queueIds;
            if (!_topicQueueIdsDict.TryGetValue(topic, out queueIds))
            {
                var queueIdsFromServer = GetTopicQueueIdsFromServer(topic).ToList();
                _topicQueueIdsDict[topic] = queueIdsFromServer;
                queueIds = queueIdsFromServer;
            }

            return queueIds;
        }
        private int GetAvailableQueueId(Message message, string routingKey)
        {
            var queueIds = GetTopicQueueIds(message.Topic);
            return _queueSelector.SelectQueueId(queueIds, message, routingKey);
        }
        private void RefreshTopicQueueCount()
        {
            foreach (var topic in _topicQueueIdsDict.Keys)
            {
                UpdateTopicQueues(topic);
            }
        }
        private void UpdateTopicQueues(string topic)
        {
            try
            {
                var topicQueueIdsFromServer = GetTopicQueueIdsFromServer(topic).ToList();
                IList<int> currentQueueIds;
                var topicQueueIdsOfLocal = _topicQueueIdsDict.TryGetValue(topic, out currentQueueIds) ? currentQueueIds : new List<int>();

                if (IsIntCollectionChanged(topicQueueIdsFromServer, topicQueueIdsOfLocal))
                {
                    _topicQueueIdsDict[topic] = topicQueueIdsFromServer;
                    _logger.DebugFormat("Queues of topic changed, producerId:{0}, topic:{1}, old queueIds:{2}, new queueIds:{3}}", Id, topic, string.Join(":", topicQueueIdsOfLocal), string.Join(":", topicQueueIdsFromServer));
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("UpdateTopicQueues has exception, producerId:{0}, topic:{1}", Id, topic), ex);
            }
        }
        private IEnumerable<int> GetTopicQueueIdsFromServer(string topic)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.GetTopicQueueIdsForProducer, Encoding.UTF8.GetBytes(topic));
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var queueIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return queueIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x));
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueIds has exception, producerId:{0}, topic:{1}, remoting response code:{2}", Id, topic, remotingResponse.Code));
            }
        }
        private RemotingRequest BuildSendMessageRequest(Message message, int queueId, string routingKey)
        {
            var request = new SendMessageRequest { Message = message, QueueId = queueId, RoutingKey = routingKey };
            var data = MessageUtils.EncodeSendMessageRequest(request);
            return new RemotingRequest((int)RequestCode.SendMessage, data);
        }
        private void StartBackgroundJobs()
        {
            _topicQueueIdsDict.Clear();
            _scheduleService.StartTask("Producer.RefreshTopicQueueCount", RefreshTopicQueueCount, Setting.UpdateTopicQueueCountInterval, Setting.UpdateTopicQueueCountInterval);
        }
        private void StopBackgroundJobs()
        {
            _scheduleService.StopTask("Producer.RefreshTopicQueueCount");
            _topicQueueIdsDict.Clear();
        }
        private bool IsIntCollectionChanged(IList<int> first, IList<int> second)
        {
            if (first.Count != second.Count)
            {
                return true;
            }
            for (var index = 0; index < first.Count; index++)
            {
                if (first[index] != second[index])
                {
                    return true;
                }
            }
            return false;
        }

        class ConnectionEventListener : IConnectionEventListener
        {
            private readonly Producer _producer;

            public ConnectionEventListener(Producer producer)
            {
                _producer = producer;
            }

            public void OnConnectionAccepted(ITcpConnection connection) { }
            public void OnConnectionFailed(SocketError socketError) { }
            public void OnConnectionClosed(ITcpConnection connection, System.Net.Sockets.SocketError socketError)
            {
                _producer.StopBackgroundJobs();
            }
            public void OnConnectionEstablished(ITcpConnection connection)
            {
                _producer.StartBackgroundJobs();
            }
        }
    }
}
