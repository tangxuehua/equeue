using System.Threading.Tasks;

namespace EQueue.Common.Extensions
{
    public static class TaskExtensions
    {
        public static TResult Wait<TResult>(this Task<TResult> task)
        {
            task.Wait();
            return task.Result;
        }
    }
}
