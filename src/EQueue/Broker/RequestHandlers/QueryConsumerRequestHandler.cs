using System.Collections.Generic;
using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryConsumerRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public QueryConsumerRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<QueryConsumerRequest>(remotingRequest.Body);
            var consumerGroup = _consumerManager.GetConsumerGroup(request.GroupName);
            var consumerIdList = new List<string>();
            if (consumerGroup != null)
            {
                consumerIdList = consumerGroup.GetConsumerIdsForTopic(request.Topic).ToList();
                consumerIdList.Sort();
            }
            var consumerIds = string.Join(",", consumerIdList);
            var data = Encoding.UTF8.GetBytes(consumerIds);
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, data);
        }
    }
}
