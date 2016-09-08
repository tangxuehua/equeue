using System;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using ECommon.Utilities;
using EQueue.Clients.Consumers;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Clients
{
    public class CommitConsumeOffsetService
    {
        #region Private Variables

        private readonly string _clientId;
        private readonly Consumer _consumer;
        private readonly ClientService _clientService;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ILogger _logger;

        #endregion

        public CommitConsumeOffsetService(Consumer consumer, ClientService clientService)
        {
            _consumer = consumer;
            _clientService = clientService;
            _clientId = clientService.GetClientId();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void CommitConsumeOffset(string brokerName, string topic, int queueId, long consumeOffset)
        {
            Ensure.NotNullOrEmpty(brokerName, "brokerName");
            Ensure.NotNullOrEmpty(topic, "topic");
            Ensure.Nonnegative(queueId, "queueId");
            Ensure.Nonnegative(consumeOffset, "consumeOffset");

            CommitConsumeOffset(new MessageQueue(brokerName, topic, queueId), consumeOffset, true);
        }
        public void CommitConsumeOffset(PullRequest pullRequest)
        {
            var consumedOffset = pullRequest.ProcessQueue.GetConsumedQueueOffset();
            if (consumedOffset >= 0)
            {
                if (!pullRequest.ProcessQueue.TryUpdatePreviousConsumedQueueOffset(consumedOffset))
                {
                    return;
                }
                CommitConsumeOffset(pullRequest.MessageQueue, consumedOffset);
            }
        }
        public void CommitConsumeOffset(MessageQueue messageQueue, long consumeOffset, bool throwIfException = false)
        {
            Ensure.NotNull(messageQueue, "messageQueue");
            Ensure.Nonnegative(consumeOffset, "consumeOffset");

            var brokerConnection = _clientService.GetBrokerConnection(messageQueue.BrokerName);
            if (brokerConnection == null)
            {
                _logger.ErrorFormat("CommitConsumeOffset failed as the target broker connection not found, messageQueue:{0}", messageQueue);
                return;
            }
            var remotingClient = brokerConnection.AdminRemotingClient;

            var request = new UpdateQueueOffsetRequest(_consumer.GroupName, messageQueue, consumeOffset);
            var remotingRequest = new RemotingRequest((int)RequestCode.UpdateQueueOffsetRequest, _binarySerializer.Serialize(request));
            var brokerAddress = remotingClient.ServerEndPoint.ToAddress();

            try
            {
                remotingClient.InvokeOneway(remotingRequest);
                if (_logger.IsDebugEnabled)
                {
                    _logger.DebugFormat("CommitConsumeOffset success, consumerGroup:{0}, consumerId:{1}, messageQueue:{2}, consumeOffset:{3}, brokerAddress:{4}",
                        _consumer.GroupName,
                        _clientId,
                        messageQueue,
                        consumeOffset,
                        brokerAddress);
                }
            }
            catch (Exception ex)
            {
                if (remotingClient.IsConnected)
                {
                    _logger.Error(string.Format("CommitConsumeOffset has exception, consumerGroup:{0}, consumerId:{1}, messageQueue:{2}, consumeOffset:{3}, brokerAddress:{4}",
                        _consumer.GroupName,
                        _clientId,
                        messageQueue,
                        consumeOffset,
                        brokerAddress), ex);
                }
                if (throwIfException)
                {
                    throw;
                }
            }
        }
    }
}
