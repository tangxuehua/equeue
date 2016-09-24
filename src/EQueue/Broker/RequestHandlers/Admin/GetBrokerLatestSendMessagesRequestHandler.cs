using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetBrokerLatestSendMessagesRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;

        public GetBrokerLatestSendMessagesRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            var latestSendMessageIds = BrokerController.Instance.GetLatestSendMessageIds();
            return RemotingResponseFactory.CreateResponse(remotingRequest, Encoding.UTF8.GetBytes(latestSendMessageIds));
        }
    }
}
