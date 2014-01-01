using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using EQueue.Infrastructure.Extensions;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Scheduling;

namespace EQueue.Clients.Consumers
{
    public class PullMessageService : IPullMessageService
    {
        private readonly BlockingCollection<PullRequest> _pullRequestBlockingQueue = new BlockingCollection<PullRequest>(new ConcurrentQueue<PullRequest>());
        private readonly Worker _executePullReqeustWorker;
        private IConsumerClient _consumerClient;
        private readonly ILogger _logger;

        public PullMessageService()
        {
            _executePullReqeustWorker = new Worker(ExecutePullRequest);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }
        public void Start(IConsumerClient consumerClient)
        {
            _consumerClient = consumerClient;
            _executePullReqeustWorker.Start();
        }
        public void EnqueuePullRequest(PullRequest pullRequest)
        {
            _pullRequestBlockingQueue.Add(pullRequest);
        }
        public void EnqueuePullRequest(PullRequest pullRequest, int millisecondsDelay)
        {
            Task.Factory.StartDelayedTask(millisecondsDelay, () => _pullRequestBlockingQueue.Add(pullRequest));
        }

        private void ExecutePullRequest()
        {
            var pullRequest = _pullRequestBlockingQueue.Take();
            IConsumer consumer = _consumerClient.GetConsumer(pullRequest.ConsumerGroup);
            if (consumer == null)
            {
                _logger.WarnFormat("Consumer not found for consumer group: {0}", pullRequest.ConsumerGroup);
                return;
            }
            try
            {
                consumer.PullMessage(pullRequest);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("ExecutePullRequest exception. PullRequest: {0}.", pullRequest), ex);
            }
        }
    }
}
