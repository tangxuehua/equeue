using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Clients.Producers
{
    public class Producer
    {
        private readonly ConcurrentDictionary<string, int> _topicQueueCountDict;
        private readonly List<int> _taskIds;
        private readonly IScheduleService _scheduleService;
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
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
            _topicQueueCountDict = new ConcurrentDictionary<string, int>();
            _taskIds = new List<int>();
            _remotingClient = new SocketRemotingClient(Setting.BrokerAddress, Setting.BrokerPort);
            _remotingClient.ClientSocketConnectionChanged += HandleRemotingClientConnectionChanged;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueSelector = ObjectContainer.Resolve<IQueueSelector>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public Producer Start()
        {
            _remotingClient.Connect();
            _remotingClient.Start();
            StartBackgroundJobs();
            _logger.InfoFormat("Started, producerId:{0}", Id);
            return this;
        }
        public Producer Shutdown()
        {
            _remotingClient.Shutdown();
            _remotingClient.ClientSocketConnectionChanged -= HandleRemotingClientConnectionChanged;
            StopBackgroundJobs();
            _logger.InfoFormat("Shutdown, producerId:{0}", Id);
            return this;
        }
        public SendResult Send(Message message, object arg)
        {
            var queueCount = GetTopicQueueCount(message.Topic);
            if (queueCount == 0)
            {
                throw new Exception(string.Format("No available queue for topic [{0}], producerId:{1}.", message.Topic, Id));
            }
            var queueId = _queueSelector.SelectQueueId(queueCount, message, arg);
            var remotingRequest = BuildSendMessageRequest(message, queueId);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, Setting.SendMessageTimeoutMilliseconds);
            var response = _binarySerializer.Deserialize<SendMessageResponse>(remotingResponse.Body);
            return new SendResult(SendStatus.Success, response.MessageOffset, response.MessageQueue, response.QueueOffset);
        }
        public Task<SendResult> SendAsync(Message message, object arg)
        {
            var queueCount = GetTopicQueueCount(message.Topic);
            if (queueCount == 0)
            {
                throw new Exception(string.Format("No available queue for topic [{0}], producerId:{1}.", message.Topic, Id));
            }
            var queueId = _queueSelector.SelectQueueId(queueCount, message, arg);
            var remotingRequest = BuildSendMessageRequest(message, queueId);
            var taskCompletionSource = new TaskCompletionSource<SendResult>();
            _remotingClient.InvokeAsync(remotingRequest, Setting.SendMessageTimeoutMilliseconds).ContinueWith((requestTask) =>
            {
                var remotingResponse = requestTask.Result;
                if (remotingResponse != null)
                {
                    var response = _binarySerializer.Deserialize<SendMessageResponse>(remotingResponse.Body);
                    var result = new SendResult(SendStatus.Success, response.MessageOffset, response.MessageQueue, response.QueueOffset);
                    taskCompletionSource.SetResult(result);
                }
                else
                {
                    var errorMessage = "Unknown error occurred when sending message to broker.";
                    if (!requestTask.IsCompleted)
                    {
                        errorMessage = "Send message to broker timeout.";
                    }
                    else if (requestTask.IsFaulted)
                    {
                        errorMessage = requestTask.Exception.Message;
                    }
                    taskCompletionSource.SetResult(new SendResult(SendStatus.Failed, errorMessage));
                }
            });
            return taskCompletionSource.Task;
        }

        private int GetTopicQueueCount(string topic)
        {
            int count;
            if (!_topicQueueCountDict.TryGetValue(topic, out count))
            {
                var countFromServer = GetTopicQueueCountFromBroker(topic);
                _topicQueueCountDict[topic] = countFromServer;
                count = countFromServer;
            }

            return count;
        }
        private void RefreshTopicQueueCount()
        {
            foreach (var topic in _topicQueueCountDict.Keys)
            {
                UpdateTopicQueueCount(topic);
            }
        }
        private void UpdateTopicQueueCount(string topic)
        {
            try
            {
                var topicQueueCountFromServer = GetTopicQueueCountFromBroker(topic);
                int count;
                var topicQueueCountOfLocal = _topicQueueCountDict.TryGetValue(topic, out count) ? count : 0;

                if (topicQueueCountFromServer != topicQueueCountOfLocal)
                {
                    _topicQueueCountDict[topic] = topicQueueCountFromServer;
                    _logger.DebugFormat("Queue count of topic updated, producerId:{0}, topic:{1}, queueCount:{2}", Id, topic, topicQueueCountFromServer);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("UpdateTopicQueueCount has exception, producerId:{0}, topic:{1}", Id, topic), ex);
            }
        }
        private int GetTopicQueueCountFromBroker(string topic)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.GetTopicQueueCount, Encoding.UTF8.GetBytes(topic));
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return BitConverter.ToInt32(remotingResponse.Body, 0);
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueCountFromBroker has exception, producerId:{0}, topic:{1}, remoting response code:{2}", Id, topic, remotingResponse.Code));
            }
        }
        private RemotingRequest BuildSendMessageRequest(Message message, int queueId)
        {
            var request = new SendMessageRequest { Message = message, QueueId = queueId };
            var data = MessageUtils.EncodeSendMessageRequest(request);
            return new RemotingRequest((int)RequestCode.SendMessage, data);
        }
        private void HandleRemotingClientConnectionChanged(bool isConnected)
        {
            if (isConnected)
            {
                StartBackgroundJobs();
            }
            else
            {
                StopBackgroundJobs();
            }
        }
        private void StartBackgroundJobs()
        {
            _taskIds.Clear();
            _taskIds.Add(_scheduleService.ScheduleTask("Producer.RefreshTopicQueueCount", RefreshTopicQueueCount, Setting.UpdateTopicQueueCountInterval, Setting.UpdateTopicQueueCountInterval));
        }
        private void StopBackgroundJobs()
        {
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }
            _taskIds.Clear();
        }
    }
}
