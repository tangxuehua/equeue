namespace EQueue.Clients.Consumers
{
    public class PullRequestSetting
    {
        public int PullThresholdForQueue { get; set; }
        public int PullTimeDelayMillsWhenFlowControl { get; set; }
        public int PullRequestTimeoutMilliseconds { get; set; }
        public int RetryMessageInterval { get; set; }
        public int PullMessageBatchSize { get; set; }

        public PullRequestSetting()
        {
            PullThresholdForQueue = 1000;
            PullTimeDelayMillsWhenFlowControl = 3000;
            PullRequestTimeoutMilliseconds = 70 * 1000;
            RetryMessageInterval = 3000;
            PullMessageBatchSize = 32;
        }

        public override string ToString()
        {
            return string.Format("[PullThresholdForQueue={0}, PullTimeDelayMillsWhenFlowControl={1}, PullRequestTimeoutMilliseconds={2}, RetryMessageInterval={3}, PullMessageBatchSize={4}]",
                PullThresholdForQueue,
                PullTimeDelayMillsWhenFlowControl,
                PullRequestTimeoutMilliseconds,
                RetryMessageInterval,
                PullMessageBatchSize);
        }
    }
}
