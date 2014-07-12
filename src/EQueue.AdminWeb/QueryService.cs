using System;
using System.Collections.Generic;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.AdminWeb
{
    public class QueryService
    {
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;

        public QueryService(IBinarySerializer binarySerializer)
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
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return _binarySerializer.Deserialize<IEnumerable<TopicQueueInfo>>(remotingResponse.Body);
            }
            else
            {
                throw new Exception(string.Format("QueryTopicQueueInfo has exception, topic:{0}}", topic));
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
                throw new Exception(string.Format("QueryTopicConsumeInfo has exception, group:{0}, topic:{1}}", group, topic));
            }
        }
    }
}