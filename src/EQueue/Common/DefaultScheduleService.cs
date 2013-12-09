using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using EQueue.Common.Logging;

namespace EQueue.Common
{
    public class DefaultScheduleService : IScheduleService
    {
        private readonly IDictionary<int, Timer> _timerDict = new ConcurrentDictionary<int, Timer>();
        private int _timerCount;
        private readonly ILogger _logger;

        public DefaultScheduleService(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.Create(GetType().Name);
        }

        public void ScheduleTask(Action action, int dueTime, int period)
        {
            var localCount = Interlocked.Increment(ref _timerCount);
            var timer = new Timer((obj) =>
            {
                var state = (TimerState)obj;
                Timer currentTimer;
                if (_timerDict.TryGetValue(state.TimerIndex, out currentTimer))
                {
                    currentTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    try
                    {
                        action();
                    }
                    catch (Exception ex)
                    {
                        _logger.Error("Schedule task has exception.", ex);
                    }
                    finally
                    {
                        currentTimer.Change(state.DueTime, state.Period);
                    }
                }
            }, new TimerState(localCount, dueTime, period), dueTime, period);

            _timerDict.Add(localCount, timer);
        }

        class TimerState
        {
            public int TimerIndex;
            public int DueTime;
            public int Period;

            public TimerState(int timerIndex, int dueTime, int period)
            {
                TimerIndex = timerIndex;
                DueTime = dueTime;
                Period = period;
            }
        }
    }
}
