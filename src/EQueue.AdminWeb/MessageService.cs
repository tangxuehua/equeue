using System;
using System.Collections.Generic;
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
            _remotingClient = new SocketRemotingClient(Settings.BrokerAddress, Settings.BrokerPort);
            _remotingClient.Connect();
            _remotingClient.Start();
            _binarySerializer = binarySerializer;
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
                throw new Exception(string.Format("QueryTopicQueueInfo has exception, topic:{0}", topic));
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
                throw new Exception(string.Format("QueryTopicConsumeInfo has exception, group:{0}, topic:{1}", group, topic));
            }
        }
        public void AddQueue(string topic)
        {
            var requestData = _binarySerializer.Serialize(new AddQueueRequest(topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.AddQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("AddQueue has exception, topic:{0}", topic));
            }
        }
        public void RemoveQueue(string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new RemoveQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.RemoveQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("RemoveQueue has exception, topic:{0}", topic));
            }
        }
        public void EnableQueue(string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new EnableQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.EnableQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("EnableQueue has exception, topic:{0}", topic));
            }
        }
        public void DisableQueue(string topic, int queueId)
        {
            var requestData = _binarySerializer.Serialize(new DisableQueueRequest(topic, queueId));
            var remotingRequest = new RemotingRequest((int)RequestCode.DisableQueue, requestData);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code != (int)ResponseCode.Success)
            {
                throw new Exception(string.Format("DisableQueue has exception, topic:{0}", topic));
            }
        }
    }
}