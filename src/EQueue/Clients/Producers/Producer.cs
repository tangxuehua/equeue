using System.Threading.Tasks;
using EQueue.Infrastructure;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;
using EQueue.Remoting.Requests;
using EQueue.Remoting.Responses;

namespace EQueue.Clients.Producers
{
    public class Producer
    {
        private const int SendMessageTimeoutMilliseconds = 3 * 1000;
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ILogger _logger;

        public Producer(string brokerAddress = "127.0.0.1", int brokerPort = 5000)
        {
            _remotingClient = new SocketRemotingClient(brokerAddress, brokerPort);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public Producer Start()
        {
            _remotingClient.Start();
            return this;
        }
        public SendResult Send(Message message, string arg)
        {
            var remotingRequest = BuildSendMessageRequest(message, arg);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, SendMessageTimeoutMilliseconds);
            var response = _binarySerializer.Deserialize<SendMessageResponse>(remotingResponse.Body);
            var sendStatus = SendStatus.Success; //TODO, figure from remotingResponse.Code;
            return new SendResult(sendStatus, response.MessageOffset, response.MessageQueue, response.QueueOffset);
        }
        public Task<SendResult> SendAsync(Message message, string arg)
        {
            var remotingRequest = BuildSendMessageRequest(message, arg);
            var taskCompletionSource = new TaskCompletionSource<SendResult>();
            _remotingClient.InvokeAsync(remotingRequest, SendMessageTimeoutMilliseconds).ContinueWith((requestTask) =>
            {
                var remotingResponse = requestTask.Result;
                if (remotingResponse != null)
                {
                    var response = _binarySerializer.Deserialize<SendMessageResponse>(remotingResponse.Body);
                    var sendStatus = SendStatus.Success; //TODO, figure from remotingResponse.Code;
                    var result = new SendResult(sendStatus, response.MessageOffset, response.MessageQueue, response.QueueOffset);
                    taskCompletionSource.SetResult(result);
                }
                else
                {
                    var result = new SendResult(SendStatus.Failed, "Send message request failed or wait for response timeout.");
                    taskCompletionSource.SetResult(result);
                }
            });
            return taskCompletionSource.Task;
        }
        public SendResult Send(Message message, string arg, Broker.IQueueSelector queueSelector)
        {
            throw new System.NotImplementedException();
        }
        public Task<SendResult> SendAsync(Message message, string arg, Broker.IQueueSelector queueSelector)
        {
            throw new System.NotImplementedException();
        }

        private RemotingRequest BuildSendMessageRequest(Message message, string arg)
        {
            var request = new SendMessageRequest { Message = message, Arg = arg };
            var data = _binarySerializer.Serialize(request);
            return new RemotingRequest((int)RequestCode.SendMessage, data);
        }
    }
}
