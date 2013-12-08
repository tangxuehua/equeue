using System.Threading.Tasks;

namespace EQueue.Client.Consumer
{
    public interface IPullMessageService
    {
        void Start();
        void ExecutePullRequestLater(PullRequest pullRequest, int millisecondsDelay);
        void ExecutePullRequestImmediately(PullRequest pullRequest);
    }
}
