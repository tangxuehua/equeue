namespace EQueue.Clients.Consumers
{
    public class PullRequestSetting
    {
        private static PullRequestSetting _default = new PullRequestSetting();

        public int PullThresholdForQueue { get; set; }
        public int ConsumeMaxSpan { get; set; }
        public int PullTimeDelayMillsWhenFlowControl { get; set; }
        public int PullRequestTimeoutMilliseconds { get; set; }
        public int PullMessageBatchSize { get; set; }

        public static PullRequestSetting Default { get { return _default; } }

        public PullRequestSetting()
        {
            PullThresholdForQueue = 1000;
            ConsumeMaxSpan = 2000;
            PullTimeDelayMillsWhenFlowControl = 100;
            PullRequestTimeoutMilliseconds = 70 * 1000;
            PullMessageBatchSize = 32;
        }

        public override string ToString()
        {
            return string.Format("[PullThresholdForQueue={0}, ConsumeMaxSpan={1}, PullTimeDelayMillsWhenFlowControl={2}, PullRequestTimeoutMilliseconds={3}, PullMessageBatchSize={4}]",
                PullThresholdForQueue,
                ConsumeMaxSpan,
                PullTimeDelayMillsWhenFlowControl,
                PullRequestTimeoutMilliseconds,
                PullMessageBatchSize);
        }
    }
}
