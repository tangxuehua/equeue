using System;

namespace EQueue.Common.Scheduling
{
    public interface IScheduleService
    {
        void ScheduleTask(Action action, int dueTime, int period);
    }
}
