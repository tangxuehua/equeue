using System.Threading.Tasks;

namespace EQueue.Infrastructure.Extensions
{
    public static class TaskExtensions
    {
        public static TResult WaitResult<TResult>(this Task<TResult> task)
        {
            task.Wait();
            return task.Result;
        }
        public static TResult WaitResult<TResult>(this Task<TResult> task, int timeoutMillis)
        {
            task.Wait(timeoutMillis);
            return task.Result;
        }
    }
}
