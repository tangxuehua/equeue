using System.Threading.Tasks;

namespace EQueue.Infrastructure.Extensions
{
    public static class TaskExtensions
    {
        public static TResult WaitResult<TResult>(this Task<TResult> task, int timeoutMillis)
        {
            if (task.Wait(timeoutMillis))
            {
                return task.Result;
            }
            return default(TResult);
        }
    }
}
