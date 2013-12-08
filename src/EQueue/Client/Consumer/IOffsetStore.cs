using System.Threading.Tasks;
using EQueue.Common;

namespace EQueue.Client.Consumer
{
    public interface IOffsetStore
    {
        void UpdateOffset(MessageQueue messageQueue, long offset);
    }
}
