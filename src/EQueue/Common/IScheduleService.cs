using System;

namespace EQueue.Common
{
    public interface IScheduleService
    {
        void ScheduleTask(Action action, int dueTime, int period);
    }
}
