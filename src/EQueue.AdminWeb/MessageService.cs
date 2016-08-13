using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.AdminWeb
{
    public class MessageService
    {
        private readonly byte[] EmptyBytes = new byte[0];
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IScheduleService _scheduleService;
        private readonly SendEmailService _sendEmailService;
        private readonly int _unconsumedMessageWarnningThreshold;
        private readonly int _checkUnconsumedMessageInterval;

        public MessageService(IBinarySerializer binarySerializer, IScheduleService scheduleService, SendEmailService sendEmailService)
        {
            _remotingClient = new SocketRemotingClient(Settings.BrokerAddress);
            _binarySerializer = binarySerializer;
            _scheduleService = scheduleService;
            _unconsumedMessageWarnningThreshold = int.Parse(ConfigurationManager.AppSettings["unconsumedMessageWarnningThreshold"]);
            _checkUnconsumedMessageInterval = int.Parse(ConfigurationManager.AppSettings["checkUnconsumedMessageInterval"]);
            _sendEmailService = sendEmailService;
        }

        public void Start()
        {
            Task.Factory.StartNew(() => _remotingClient.Start());
            if (bool.Parse(ConfigurationManager.AppSettings["enableMonitor"]))
            {
                _scheduleService.StartTask("CheckUnconsumedMessages", CheckUnconsumedMessages, 1000, _checkUnconsumedMessageInterval);
            }
        }
        public BrokerStatisticInfo QueryBrokerStatisticInfo()
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryBrokerStatisticInfo, new byte[0]);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<BrokerStatisticInfo>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("QueryBrokerStatisticInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void CreateTopic(string topic, int initialQueueCount)
        {
            var requestData = _binarySerializer.Serialize(new CreateTopicRequest(topic, initialQueueCount));
            var remotingRequest = new RemotingRequest((int)RequestCode.CreateTopic, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("CreateTopic failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteTopic(string topic)
        {
            var requestData = _binarySerializer.Serialize(new DeleteTopicRequest(topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.DeleteTopic, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteTopic failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<TopicQueueInfo> GetTopicQueueInfo(string topic)
        {
            var requestData = _binarySerializer.Serialize(new QueryTopicQueueInfoRequest(topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryTopicQueueInfo, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<TopicQueueInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<string> GetProducerInfo()
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryProducerInfo, EmptyBytes);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var producerIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return producerIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                throw new Exception(string.Format("GetProducerInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public IEnumerable<ConsumerInfo> GetConsumerInfo(string group, string topic)
        {
            var requestData = _binarySerializer.Serialize(new QueryConsumerInfoRequest(group, topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryConsumerInfo, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<ConsumerInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("GetConsumerInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void AddQueue(string topic)
        {
            var requestData = _binarySerializer.Serialize(new AddQueueRequest(topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.AddQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("AddQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteQueue(string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new DeleteQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.DeleteQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueProducerVisible(string topic, int queueId, bool visible)
        {
            var requestData = _binarySerializer.Serialize(new SetQueueProducerVisibleRequest(topic, queueId, visible));
            var remotingRequest = new RemotingRequest((int)RequestCode.SetProducerVisible, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueProducerVisible failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueConsumerVisible(string topic, int queueId, bool visible)
        {
            var requestData = _binarySerializer.Serialize(new SetQueueConsumerVisibleRequest(topic, queueId, visible));
            var remotingRequest = new RemotingRequest((int)RequestCode.SetConsumerVisible, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueConsumerVisible failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public QueueMessage GetMessageDetail(string messageId)
        {
            var requestData = _binarySerializer.Serialize(new GetMessageDetailRequest(messageId));
            var remotingRequest = new RemotingRequest((int)RequestCode.GetMessageDetail, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<QueueMessage>>(remotingResponse.Body).SingleOrDefault();
            }
            else
            {
                throw new Exception(string.Format("GetMessageDetail failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void SetQueueNextConsumeOffset(string consumerGroup, string topic, int queueId, long nextOffset)
        {
            if (nextOffset < 0)
            {
                throw new ArgumentException("nextOffset cannot be small than zero.");
            }
            var requestData = _binarySerializer.Serialize(new SetQueueNextConsumeOffsetRequest(consumerGroup, topic, queueId, nextOffset));
            var remotingRequest = new RemotingRequest((int)RequestCode.SetQueueNextConsumeOffset, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("SetQueueNextConsumeOffset failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DeleteConsumerGroup(string consumerGroup)
        {
            if (string.IsNullOrEmpty(consumerGroup))
            {
                throw new ArgumentException("consumerGroup cannot be null or empty.");
            }
            var requestData = _binarySerializer.Serialize(new DeleteConsumerGroupRequest(consumerGroup));
            var remotingRequest = new RemotingRequest((int)RequestCode.DeleteConsumerGroup, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("DeleteConsumerGroup failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }

        private void CheckUnconsumedMessages()
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryBrokerStatisticInfo, new byte[0]);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var statisticInfo = _binarySerializer.Deserialize<BrokerStatisticInfo>(remotingResponse.Body);
                if (statisticInfo.TotalUnConsumedMessageCount >= _unconsumedMessageWarnningThreshold)
                {
                    _sendEmailService.SendTooManyMessageNotConsumedNotification(statisticInfo.TotalUnConsumedMessageCount);
                }
            }
            else
            {
                throw new Exception(string.Format("QueryBrokerStatisticInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
    }
}