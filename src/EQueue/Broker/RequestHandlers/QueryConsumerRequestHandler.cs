using System.Collections.Generic;
using System.Linq;
using System.Text;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Remoting;

namespace EQueue.Broker.Processors
{
    public class QueryConsumerRequestHandler : IRequestHandler
    {
        private BrokerController _brokerController;
        private ILogger _logger;

        public QueryConsumerRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var groupName = Encoding.UTF8.GetString(request.Body);
            var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(groupName);
            var consumerIdList = new List<string>();
            if (consumerGroup != null)
            {
                consumerIdList = consumerGroup.GetAllConsumerChannels().Select(x => x.ClientId).ToList();
                consumerIdList.Sort();
            }
            var consumerIds = string.Join(",", consumerIdList);
            var data = Encoding.UTF8.GetBytes(consumerIds);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, data);
        }
    }
}
