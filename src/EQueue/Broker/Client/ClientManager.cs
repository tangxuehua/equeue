using ECommon.IoC;
using ECommon.Scheduling;

namespace EQueue.Broker.Client
{
    public class ClientManager
    {
        private BrokerController _brokerController;
        private readonly IScheduleService _scheduleService;
        private int _checkNotActiveConsumerTaskId;

        public ClientManager(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
        }

        public void Start()
        {
            _checkNotActiveConsumerTaskId = _scheduleService.ScheduleTask(CheckNotActiveConsumer, 10 * 1000, 10 * 1000);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_checkNotActiveConsumerTaskId);
        }

        private void CheckNotActiveConsumer()
        {
            _brokerController.ConsumerManager.ScanNotActiveConsumer();
        }
    }
}
