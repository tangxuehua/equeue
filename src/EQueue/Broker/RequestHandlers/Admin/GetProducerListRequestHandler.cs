using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.Exceptions;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetProducerListRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private ProducerManager _producerManager;

        public GetProducerListRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _producerManager = ObjectContainer.Resolve<ProducerManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            var producerIdList = _producerManager.GetAllProducers();
            var data = Encoding.UTF8.GetBytes(string.Join(",", producerIdList));
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
