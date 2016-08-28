using System.Threading;

namespace EQueue.Utils
{
    public class DefaultRTStatisticService : IRTStatisticService
    {
        private long _totalCount;
        private long _totalTime;

        public void AddRT(double rtMilliseconds)
        {
            Interlocked.Increment(ref _totalCount);
            Interlocked.Add(ref _totalTime, (long)(rtMilliseconds * 1000));
        }
        public double ResetAndGetRTStatisticInfo()
        {
            var totalCount = _totalCount;
            var totalTime = _totalTime;
            _totalCount = 0L;
            _totalTime = 0L;

            if (totalCount == 0)
            {
                return 0;
            }

            return ((double)totalTime / 1000) / totalCount;
        }
    }
}
