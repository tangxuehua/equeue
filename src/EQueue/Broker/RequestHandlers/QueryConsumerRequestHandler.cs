using System.Collections.Generic;
using System.Linq;
using System.Text;
using EQueue.Remoting;

namespace EQueue.Broker.Processors
{
    public class QueryConsumerRequestHandler : IRequestHandler
    {
        private BrokerController _brokerController;

        public QueryConsumerRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var groupName = Encoding.UTF8.GetString(request.Body);
            var consumerGroup = _brokerController.ConsumerManager.GetConsumerGroup(groupName);
            var consumerIdList = new List<string>();
            if (consumerGroup != null)
            {
                consumerIdList = consumerGroup.GetAllConsumerChannels().Select(x => x.ClientId).ToList();
            }
            var data = Encoding.UTF8.GetBytes(string.Join(",", consumerIdList));
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, data);
        }
    }
}
