using EQueue.Broker.LongPolling;
using EQueue.Broker.Processors;
using EQueue.Infrastructure.IoC;
using EQueue.Remoting;

namespace EQueue.Broker
{
    public class BrokerController
    {
        private readonly IMessageService _messageService;
        private readonly SocketRemotingServer _remotingServer;
        public PullRequestHoldService PullRequestHoldService { get; private set; }

        public BrokerController(string address = "127.0.0.1", int port = 5000, int backlog = 5000)
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _remotingServer = new SocketRemotingServer(address, port, backlog);
            PullRequestHoldService = new PullRequestHoldService();
        }

        public BrokerController Initialize()
        {
            _remotingServer.RegisterRequestProcessor((int)RequestCode.SendMessage, new SendMessageRequestProcessor());
            _remotingServer.RegisterRequestProcessor((int)RequestCode.PullMessage, new PullMessageRequestProcessor(this));
            return this;
        }
        public void Start()
        {
            _remotingServer.Start();
            PullRequestHoldService.Start();
        }
        public void Shutdown()
        {

        }
    }
}
