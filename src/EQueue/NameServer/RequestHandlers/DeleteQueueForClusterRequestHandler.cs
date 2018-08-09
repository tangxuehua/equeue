using System;
using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class DeleteQueueForClusterRequestHandler : IRequestHandler
    {
        private NameServerController _nameServerController;
        private IBinarySerializer _binarySerializer;

        public DeleteQueueForClusterRequestHandler(NameServerController nameServerController)
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _nameServerController = nameServerController;
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<DeleteQueueForClusterRequest>(remotingRequest.Body);
            var requestService = new BrokerRequestService(_nameServerController);

            requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, async remotingClient =>
            {
                var requestData = _binarySerializer.Serialize(new DeleteQueueRequest(request.Topic, request.QueueId));
                var remotingResponse = await remotingClient.InvokeAsync(new RemotingRequest((int)BrokerRequestCode.DeleteQueue, requestData), 30000);
                context.SendRemotingResponse(remotingResponse);
            });

            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
