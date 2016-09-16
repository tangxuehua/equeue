using System;
using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class DeleteConsumerGroupRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IConsumeOffsetStore _offsetStore;
        private ConsumerManager _consumerManager;

        public DeleteConsumerGroupRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = _binarySerializer.Deserialize<DeleteConsumerGroupRequest>(remotingRequest.Body);

            if (string.IsNullOrEmpty(request.GroupName))
            {
                throw new ArgumentException("DeleteConsumerGroupRequest.GroupName cannot be null or empty.");
            }
            var consumerGroup = _consumerManager.GetConsumerGroup(request.GroupName);
            if (consumerGroup != null && consumerGroup.GetConsumerCount() > 0)
            {
                throw new Exception("Consumer group has consumer exist, not allowed to delete.");
            }

            var success = _offsetStore.DeleteConsumerGroup(request.GroupName);

            return RemotingResponseFactory.CreateResponse(remotingRequest, Encoding.UTF8.GetBytes(success ? "1" : "0"));
        }
    }
}
