using System;

namespace EQueue.Infrastructure.Scheduling
{
    public interface IScheduleService
    {
        void ScheduleTask(Action action, int dueTime, int period);
    }
}
