using System.Threading.Tasks;

namespace EQueue.Clients.Producers
{
    public enum SendStatus
    {
        Success = 1,
        Timeout,
        Failed
    }
}
