using System.Threading.Tasks;
using EQueue.Common;

namespace EQueue.Clients.Producers
{
    public interface IProducer
    {
        string GroupName { get; }
        void Start();
        void Shutdown();
        SendResult Send(Message message, object hashKey);
        Task<SendResult> SendAsync(Message message, object hashKey);
    }
}
