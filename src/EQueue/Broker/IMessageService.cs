using EQueue.Common;

namespace EQueue.Broker
{
    public interface IMessageService
    {
        MessageStoreResult StoreMessage(Message message, object arg);
    }
}
