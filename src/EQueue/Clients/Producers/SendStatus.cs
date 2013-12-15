using System.Threading.Tasks;

namespace EQueue.Clients.Producers
{
    public enum SendStatus
    {
        SEND_OK = 1,
        FLUSH_DISK_TIMEOUT
    }
}
