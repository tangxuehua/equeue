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
            _remotingClient = new SocketRemotingClient(new IPEndPoint(Settings.BrokerAddress, Settings.BrokerPort));
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
        public IEnumerable<TopicConsumeInfo> GetTopicConsumeInfo(string group, string topic)
        {
            var requestData = _binarySerializer.Serialize(new QueryTopicConsumeInfoRequest(group, topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryTopicConsumeInfo, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<TopicConsumeInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("QueryTopicConsumeInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
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
        public void RemoveQueue(string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new RemoveQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.RemoveQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("RemoveQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void EnableQueue(string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new EnableQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.EnableQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("EnableQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void DisableQueue(string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new DisableQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.DisableQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("DisableQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public void RemoveQueueOffsetInfo(string consumerGroup, string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new RemoveQueueOffsetInfoRequest(consumerGroup, topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.RemoveQueueOffsetInfo, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("RemoveQueueOffsetInfo failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public QueryMessageResponse QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize)
        {
            var request = new QueryMessageRequest(topic, queueId, code, routingKey, pageIndex, pageSize);
            var requestData = _binarySerializer.Serialize(request);
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryMessage, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<QueryMessageResponse>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("QueryMessages failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.Body)));
            }
        }
        public QueueMessage GetMessageDetail(long? messageOffset, string messageId)
        {
            var requestData = _binarySerializer.Serialize(new GetMessageDetailRequest(messageOffset, messageId));
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