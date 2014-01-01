using System.Threading.Tasks;
using EQueue.Infrastructure;
using EQueue.Infrastructure.Extensions;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;
using EQueue.Remoting.Requests;
using EQueue.Remoting.Responses;

namespace EQueue.Clients.Producers
{
    public class Producer : IProducer
    {
        private readonly string BrokerAddress;
        private readonly IRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ILogger _logger;

        #region Constructors

        public Producer(string brokerAddress, IRemotingClient remotingClient, IBinarySerializer binarySerializer, ILoggerFactory loggerFactory)
        {
            BrokerAddress = brokerAddress;
            _remotingClient = remotingClient;
            _binarySerializer = binarySerializer;
            _logger = loggerFactory.Create(GetType().Name);
        }

        #endregion

        public SendResult Send(Message message, string arg)
        {
            return SendAsync(message, arg).Wait<SendResult>();
        }
        public Task<SendResult> SendAsync(Message message, string arg)
        {
            var request = new SendMessageRequest { Message = message, Arg = arg };
            var data = _binarySerializer.Serialize(request);
            var remotingRequest = new RemotingRequest((int)RequestCode.SendMessage, data);
            var taskCompletionSource = new TaskCompletionSource<SendResult>();
            _remotingClient.InvokeAsync(BrokerAddress, remotingRequest, 3000).ContinueWith((requestTask) =>
            {
                var remotingResponse = requestTask.Result;
                var response = _binarySerializer.Deserialize<SendMessageResponse>(remotingResponse.Body);
                var sendStatus = SendStatus.Success; //TODO, figure from remotingResponse.Code;
                var result = new SendResult(sendStatus, response.MessageId, response.MessageOffset, response.MessageQueue, response.QueueOffset);
                taskCompletionSource.SetResult(result);
            });
            return taskCompletionSource.Task;
        }
    }
}
