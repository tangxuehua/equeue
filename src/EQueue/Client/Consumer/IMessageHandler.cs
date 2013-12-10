using EQueue.Common;

namespace EQueue
{
    public interface IMessageHandler
    {
        void Handle(Message message);
    }
}
