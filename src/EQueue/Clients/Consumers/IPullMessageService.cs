namespace EQueue.Clients.Consumers
{
    public interface IPullMessageService
    {
        void Start(IConsumerClient consumerClient);
        void EnqueuePullRequest(PullRequest pullRequest);
        void EnqueuePullRequest(PullRequest pullRequest, int millisecondsDelay);
    }
}
