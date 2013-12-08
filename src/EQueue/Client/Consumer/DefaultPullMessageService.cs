using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using EQueue.Common;
using EQueue.Common.Logging;
using EQueue.Common.Extensions;

namespace EQueue.Client.Consumer
{
    public class DefaultPullMessageService : IPullMessageService
    {
        private readonly ILogger _logger;
        private readonly Worker _worker;
        private readonly DefaultClient _client;
        private readonly BlockingCollection<PullRequest> _pullRequestQueue = new BlockingCollection<PullRequest>(new ConcurrentQueue<PullRequest>());

        public DefaultPullMessageService(DefaultClient client, ILoggerFactory loggerFactory)
        {
            _client = client;
            _logger = loggerFactory.Create(GetType().Name);
            _worker = new Worker(() =>
            {
                var pullRequest = _pullRequestQueue.Take();
                if (pullRequest == null) return;
                try
                {
                    _client.SelectConsumer(pullRequest).PullMessage(pullRequest);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Pull message has exception. PullRequest: {0}.", pullRequest), ex);
                    //TODO, to check if we need this code here.
                    //_pullRequestQueue.Add(pullRequest);
                }
            });
        }

        public void Start()
        {
            _worker.Start();
        }

        public void ExecutePullRequestLater(PullRequest pullRequest, int millisecondsDelay)
        {
            Task.Factory.StartDelayedTask(millisecondsDelay, () => _pullRequestQueue.Add(pullRequest));
        }

        public void ExecutePullRequestImmediately(PullRequest pullRequest)
        {
            _pullRequestQueue.Add(pullRequest);
        }
    }
}
