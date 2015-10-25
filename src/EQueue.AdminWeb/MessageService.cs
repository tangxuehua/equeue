using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.AdminWeb
{
    public class MessageService
    {
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;

        public MessageService(IBinarySerializer binarySerializer)
        {
            _remotingClient = new SocketRemotingClient(Settings.BrokerAddress);
            _binarySerializer = binarySerializer;
        }

        public void Start()
        {
            Task.Factory.StartNew(() => _remotingClient.Start());
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
                throw new Exception(string.Format("QueryTopicQueueInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
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
                throw new Exception(string.Format("QueryConsumerInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
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
    }
}