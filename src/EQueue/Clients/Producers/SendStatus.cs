using System.Threading.Tasks;

namespace EQueue.Clients.Producers
{
    public enum SendStatus
    {
        Success = 1,
        FlushDiskTimeout = 2,
        Failed = 1000
    }
}
