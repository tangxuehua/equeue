namespace EQueue.Utils
{
    public interface IRTStatisticService
    {
        void AddRT(double rtTime);
        double ResetAndGetRTStatisticInfo();
    }
}
