using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Utilities;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Clients.Producers
{
    public class Producer
    {
        #region Private Variables

        private readonly object _lockObj = new object();
        private readonly IQueueSelector _queueSelector;
        private readonly ILogger _logger;
        private bool _started;

        #endregion

        public string Name { get; private set; }
        public ProducerSetting Setting { get; private set; }
        public IResponseHandler ResponseHandler { get; private set; }
        public ClientService ClientService { get; }

        public Producer(string name = "DefaultProducer") : this(null, name) { }
        public Producer(ProducerSetting setting = null, string name = "DefaultProducer")
        {
            Name = name;
            Setting = setting ?? new ProducerSetting();

            if (Setting.NameServerList == null || Setting.NameServerList.Count() == 0)
            {
                throw new Exception("Name server address is not specified.");
            }

            _queueSelector = ObjectContainer.Resolve<IQueueSelector>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            var clientSetting = new ClientSetting
            {
                ClientName = Name,
                ClusterName = Setting.ClusterName,
                NameServerList = Setting.NameServerList,
                SocketSetting = Setting.SocketSetting,
                OnlyFindMasterBroker = true,
                SendHeartbeatInterval = Setting.HeartbeatBrokerInterval,
                RefreshBrokerAndTopicRouteInfoInterval = Setting.RefreshBrokerAndTopicRouteInfoInterval
            };
            ClientService = new ClientService(clientSetting, this, null);

            TaskScheduler.UnobservedTaskException += (sender, ex) =>
            {
                _logger.ErrorFormat("UnobservedTaskException occurred.", ex);
            };
        }

        #region Public Methods

        public Producer RegisterResponseHandler(IResponseHandler responseHandler)
        {
            ResponseHandler = responseHandler;
            return this;
        }
        public Producer Start()
        {
            ClientService.Start();
            _started = true;
            _logger.InfoFormat("Producer startted.");
            return this;
        }
        public Producer Shutdown()
        {
            ClientService.Stop();
            _logger.Info("Producer shutdown.");
            return this;
        }
        public async Task<SendResult> SendAsync(Message message, string routingKey, int timeoutMilliseconds = 30 * 1000)
        {
            Ensure.NotNull(message, "message");
            if (!_started)
            {
                throw new Exception("Producer not started, please start the producer first.");
            }

            var sendResult = default(SendResult);
            var retryCount = 0;
            while (retryCount <= Setting.SendMessageMaxRetryCount)
            {
                var messageQueueInfo = await TryGetAvailableMessageQueue(message, routingKey);
                if (messageQueueInfo == null)
                {
                    throw new Exception(string.Format("No available message queue for topic [{0}]", message.Topic));
                }

                var messageQueue = messageQueueInfo.MessageQueue;
                var brokerConnection = messageQueueInfo.BrokerConnection;

                var remotingRequest = BuildSendMessageRequest(message, messageQueue.QueueId, brokerConnection);
                try
                {
                    var remotingResponse = await messageQueueInfo.BrokerConnection.RemotingClient.InvokeAsync(remotingRequest, timeoutMilliseconds).ConfigureAwait(false);
                    if (remotingResponse == null)
                    {
                        sendResult = new SendResult(SendStatus.Timeout, null, string.Format("Send message timeout, message: {0}, routingKey: {1}, timeoutMilliseconds: {2}, brokerInfo: {3}", message, routingKey, timeoutMilliseconds, brokerConnection.BrokerInfo));
                    }
                    return ParseSendResult(remotingResponse);
                }
                catch (Exception ex)
                {
                    sendResult = new SendResult(SendStatus.Failed, null, ex.Message);
                }
                if (sendResult.SendStatus == SendStatus.Success)
                {
                    return sendResult;
                }
                if (retryCount > 0)
                {
                    _logger.ErrorFormat("Send message failed, queue: {0}, broker: {1}, sendResult: {2}, retryTimes: {3}", messageQueue, brokerConnection.BrokerInfo, sendResult, retryCount);
                }
                else
                {
                    _logger.ErrorFormat("Send message failed, queue: {0}, broker: {1}, sendResult: {2}", messageQueue, brokerConnection.BrokerInfo, sendResult);
                }

                retryCount++;
            }
            return sendResult;
        }
        public async Task<BatchSendResult> BatchSendAsync(IEnumerable<Message> messages, string routingKey, int timeoutMilliseconds = 30 * 1000)
        {
            Ensure.NotNull(messages, "messages");
            if (messages.Count() == 0)
            {
                throw new Exception("Batch message must contains at least one message.");
            }
            if (!_started)
            {
                throw new Exception("Producer not started, please start the producer first.");
            }

            var message = messages.First();
            var sendResult = default(BatchSendResult);
            var retryCount = 0;
            while (retryCount <= Setting.SendMessageMaxRetryCount)
            {
                var messageQueueInfo = await TryGetAvailableMessageQueue(message, routingKey);
                if (messageQueueInfo == null)
                {
                    throw new Exception(string.Format("No available message queue for topic [{0}]", message.Topic));
                }

                var messageQueue = messageQueueInfo.MessageQueue;
                var brokerConnection = messageQueueInfo.BrokerConnection;

                var remotingRequest = BuildBatchSendMessageRequest(messages, messageQueue.QueueId, brokerConnection);
                try
                {
                    var remotingResponse = await brokerConnection.RemotingClient.InvokeAsync(remotingRequest, timeoutMilliseconds).ConfigureAwait(false);
                    if (remotingResponse == null)
                    {
                        sendResult = new BatchSendResult(SendStatus.Timeout, null, string.Format("Batch send message timeout, queue: {0}, routingKey: {1}, timeoutMilliseconds: {2}, brokerInfo: {3}", messageQueue, routingKey, timeoutMilliseconds, brokerConnection.BrokerInfo));
                    }
                    return ParseBatchSendResult(remotingResponse);
                }
                catch (Exception ex)
                {
                    sendResult = new BatchSendResult(SendStatus.Failed, null, ex.Message);
                }
                if (sendResult.SendStatus == SendStatus.Success)
                {
                    return sendResult;
                }
                if (retryCount > 0)
                {
                    _logger.ErrorFormat("Batch send message failed, queue: {0}, routingKey: {1}, broker: {2}, sendResult: {3}, retryTimes: {4}", messageQueue, routingKey, brokerConnection.BrokerInfo, sendResult, retryCount);
                }
                else
                {
                    _logger.ErrorFormat("Batch send message failed, queue: {0}, routingKey: {1}, broker: {2}, sendResult: {3}", messageQueue, routingKey, brokerConnection.BrokerInfo, sendResult);
                }

                retryCount++;
            }
            return sendResult;
        }
        public async Task SendWithCallback(Message message, string routingKey)
        {
            Ensure.NotNull(message, "message");
            if (!_started)
            {
                throw new Exception("Producer not started, please start the producer first.");
            }

            var messageQueueInfo = await TryGetAvailableMessageQueue(message, routingKey);
            if (messageQueueInfo == null)
            {
                throw new Exception(string.Format("No available message queue for topic [{0}]", message.Topic));
            }

            var messageQueue = messageQueueInfo.MessageQueue;
            var brokerConnection = messageQueueInfo.BrokerConnection;

            var remotingRequest = BuildSendMessageRequest(message, messageQueue.QueueId, brokerConnection);

            brokerConnection.RemotingClient.InvokeWithCallback(remotingRequest);
        }
        public async Task BatchSendWithCallback(IEnumerable<Message> messages, string routingKey)
        {
            Ensure.NotNull(messages, "messages");
            if (messages.Count() == 0)
            {
                throw new Exception("Batch message must contains at least one message.");
            }
            if (!_started)
            {
                throw new Exception("Producer not started, please start the producer first.");
            }

            var message = messages.First();
            var messageQueueInfo = await TryGetAvailableMessageQueue(message, routingKey);
            if (messageQueueInfo == null)
            {
                throw new Exception(string.Format("No available message queue for topic [{0}]", message.Topic));
            }

            var messageQueue = messageQueueInfo.MessageQueue;
            var brokerConnection = messageQueueInfo.BrokerConnection;

            var remotingRequest = BuildBatchSendMessageRequest(messages, messageQueue.QueueId, brokerConnection);

            brokerConnection.RemotingClient.InvokeWithCallback(remotingRequest);
        }
        public async Task SendOneway(Message message, string routingKey)
        {
            Ensure.NotNull(message, "message");
            if (!_started)
            {
                throw new Exception("Producer not started, please start the producer first.");
            }

            var messageQueueInfo = await TryGetAvailableMessageQueue(message, routingKey);
            if (messageQueueInfo == null)
            {
                throw new Exception(string.Format("No available message queue for topic [{0}]", message.Topic));
            }

            var messageQueue = messageQueueInfo.MessageQueue;
            var brokerConnection = messageQueueInfo.BrokerConnection;

            var remotingRequest = BuildSendMessageRequest(message, messageQueue.QueueId, brokerConnection);

            brokerConnection.RemotingClient.InvokeOneway(remotingRequest);
        }
        public async Task BatchSendOneway(IEnumerable<Message> messages, string routingKey)
        {
            Ensure.NotNull(messages, "messages");
            if (messages.Count() == 0)
            {
                throw new Exception("Batch message must contains at least one message.");
            }
            if (!_started)
            {
                throw new Exception("Producer not started, please start the producer first.");
            }

            var message = messages.First();
            var messageQueueInfo = await TryGetAvailableMessageQueue(message, routingKey);
            if (messageQueueInfo == null)
            {
                throw new Exception(string.Format("No available message queue for topic [{0}]", message.Topic));
            }

            var messageQueue = messageQueueInfo.MessageQueue;
            var brokerConnection = messageQueueInfo.BrokerConnection;

            var remotingRequest = BuildBatchSendMessageRequest(messages, messageQueue.QueueId, brokerConnection);

            brokerConnection.RemotingClient.InvokeOneway(remotingRequest);
        }
        public static SendResult ParseSendResult(RemotingResponse remotingResponse)
        {
            Ensure.NotNull(remotingResponse, "remotingResponse");

            if (remotingResponse.ResponseCode == ResponseCode.Success)
            {
                var result = MessageUtils.DecodeMessageStoreResult(remotingResponse.ResponseBody);
                return new SendResult(SendStatus.Success, result, null);
            }
            else if (remotingResponse.ResponseCode == 0)
            {
                return new SendResult(SendStatus.Timeout, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
            else
            {
                return new SendResult(SendStatus.Failed, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
        }
        public static BatchSendResult ParseBatchSendResult(RemotingResponse remotingResponse)
        {
            Ensure.NotNull(remotingResponse, "remotingResponse");

            if (remotingResponse.ResponseCode == ResponseCode.Success)
            {
                var result = BatchMessageUtils.DecodeMessageStoreResult(remotingResponse.ResponseBody);
                return new BatchSendResult(SendStatus.Success, result, null);
            }
            else if (remotingResponse.ResponseCode == 0)
            {
                return new BatchSendResult(SendStatus.Timeout, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
            else
            {
                return new BatchSendResult(SendStatus.Failed, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
        }
        public IList<MessageQueue> GetAvailableMessageQueues(string topic)
        {
            return ClientService.GetAvailableMessageQueues(topic);
        }

        #endregion

        internal void SendHeartbeat()
        {
            var brokerConnections = ClientService.GetAllBrokerConnections();

            foreach (var brokerConnection in brokerConnections)
            {
                var remotingClient = brokerConnection.RemotingClient;
                var clientId = ClientService.GetClientId();

                try
                {
                    var data = Encoding.UTF8.GetBytes(clientId);
                    remotingClient.InvokeOneway(new RemotingRequest((int)BrokerRequestCode.ProducerHeartbeat, data));
                }
                catch (Exception ex)
                {
                    if (remotingClient.IsConnected)
                    {
                        _logger.Error(string.Format("Send producer heartbeat has exception, brokerInfo: {0}", brokerConnection.BrokerInfo), ex);
                    }
                }
            }
        }

        #region Private Methods

        private async Task<MessageQueueInfo> TryGetAvailableMessageQueue(Message message, string routingKey)
        {
            var messageQueue = await GetAvailableMessageQueueAsync(message, routingKey);
            if (messageQueue == null)
            {
                _logger.ErrorFormat("No available message queue for topic [{0}]", message.Topic);
                return null;
            }
            else
            {
                var brokerConnection = ClientService.GetBrokerConnection(messageQueue.BrokerName);
                if (brokerConnection != null && brokerConnection.RemotingClient.IsConnected)
                {
                    return new MessageQueueInfo
                    {
                        MessageQueue = messageQueue,
                        BrokerConnection = brokerConnection
                    };
                }
                _logger.ErrorFormat("Broker is unavailable for queue [{0}]", messageQueue);
                return null;
            }
        }
        private async Task<MessageQueue> GetAvailableMessageQueueAsync(Message message, string routingKey)
        {
            var messageQueueList = await ClientService.GetTopicMessageQueuesAsync(message.Topic);
            if (messageQueueList == null || messageQueueList.IsEmpty())
            {
                return null;
            }
            return _queueSelector.SelectMessageQueue(messageQueueList, message, routingKey);
        }
        private RemotingRequest BuildSendMessageRequest(Message message, int queueId, BrokerConnection brokerConnection)
        {
            var request = new SendMessageRequest
            {
                Message = message,
                QueueId = queueId,
                ProducerAddress = brokerConnection.RemotingClient.LocalEndPoint.ToAddress()
            };
            var data = MessageUtils.EncodeSendMessageRequest(request);
            if (data.Length > Setting.MessageMaxSize)
            {
                throw new Exception("Message size cannot exceed max message size:" + Setting.MessageMaxSize);
            }
            return new RemotingRequest((int)BrokerRequestCode.SendMessage, data);
        }
        private RemotingRequest BuildBatchSendMessageRequest(IEnumerable<Message> messages, int queueId, BrokerConnection brokerConnection)
        {
            var request = new BatchSendMessageRequest
            {
                Messages = messages,
                QueueId = queueId,
                ProducerAddress = brokerConnection.RemotingClient.LocalEndPoint.ToAddress()
            };
            var data = BatchMessageUtils.EncodeSendMessageRequest(request);
            if (data.Length > Setting.MessageMaxSize * messages.Count())
            {
                throw new Exception("Message size cannot exceed max message size:" + Setting.MessageMaxSize);
            }
            return new RemotingRequest((int)BrokerRequestCode.BatchSendMessage, data);
        }

        class MessageQueueInfo
        {
            public MessageQueue MessageQueue { get; set; }
            public BrokerConnection BrokerConnection { get; set; }
        }

        #endregion
    }
}
