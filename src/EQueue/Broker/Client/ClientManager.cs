using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Scheduling;

namespace EQueue.Broker.Client
{
    public class ClientManager
    {
        private BrokerController _brokerController;
        private readonly IScheduleService _scheduleService;
        private int _checkNotActiveProducerTaskId;
        private int _checkNotActiveConsumerTaskId;

        public ClientManager(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
        }

        public void Start()
        {
            _checkNotActiveProducerTaskId = _scheduleService.ScheduleTask(CheckNotActiveProducer, 10 * 1000, 10 * 1000);
            _checkNotActiveConsumerTaskId = _scheduleService.ScheduleTask(CheckNotActiveConsumer, 10 * 1000, 10 * 1000);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_checkNotActiveProducerTaskId);
            _scheduleService.ShutdownTask(_checkNotActiveConsumerTaskId);
        }

        private void CheckNotActiveProducer()
        {
            _brokerController.ProducerManager.ScanNotActiveProducer();
        }
        private void CheckNotActiveConsumer()
        {
            _brokerController.ConsumerManager.ScanNotActiveConsumer();
        }
    }
}
