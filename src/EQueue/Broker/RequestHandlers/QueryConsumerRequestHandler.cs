using System.Collections.Generic;
using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.Exceptions;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers
{
    public class QueryConsumerRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private IBinarySerializer _binarySerializer;

        public QueryConsumerRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                return RemotingResponseFactory.CreateResponse(remotingRequest, Encoding.UTF8.GetBytes(string.Empty));
            }

            var request = _binarySerializer.Deserialize<QueryConsumerRequest>(remotingRequest.Body);
            var consumerGroup = _consumerManager.GetConsumerGroup(request.GroupName);
            var consumerIdList = new List<string>();
            if (consumerGroup != null)
            {
                consumerIdList = consumerGroup.GetConsumerIdsForTopic(request.Topic).ToList();
                consumerIdList.Sort();
            }
            return RemotingResponseFactory.CreateResponse(remotingRequest, Encoding.UTF8.GetBytes(string.Join(",", consumerIdList)));
        }
    }
}
