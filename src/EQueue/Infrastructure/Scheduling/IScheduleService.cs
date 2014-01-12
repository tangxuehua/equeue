using System;

namespace EQueue.Infrastructure.Scheduling
{
    public interface IScheduleService
    {
        int ScheduleTask(Action action, int dueTime, int period);
        void ShutdownTask(int taskId);
    }
}
