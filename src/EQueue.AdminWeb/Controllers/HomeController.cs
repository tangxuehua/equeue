using System.Text;
using System.Web.Mvc;
using EQueue.AdminWeb.Models;

namespace EQueue.AdminWeb.Controllers
{
    public class HomeController : Controller
    {
        private MessageService _messageService;

        public HomeController(MessageService messageService)
        {
            _messageService = messageService;
        }

        public ActionResult Index()
        {
            var result = _messageService.QueryBrokerStatisticInfo();
            return View(new BrokerStatisticInfoViewModel
            {
                TopicCount = result.TopicCount,
                QueueCount = result.QueueCount,
                ConsumerGroupCount = result.ConsumerGroupCount,
                ConsumerCount = result.ConsumerCount,
                TotalUnConsumedMessageCount = result.TotalUnConsumedMessageCount,
                MessageChunkCount = result.MessageChunkCount,
                MessageMaxChunkNum = result.MessageMaxChunkNum,
                MessageMinChunkNum = result.MessageMinChunkNum
            });
        }
        public ActionResult CreateTopic(string topic, int? initialQueueCount)
        {
            _messageService.CreateTopic(topic, initialQueueCount ?? 4);
            return RedirectToAction("QueueInfo");
        }
        public ActionResult DeleteTopic(string topic)
        {
            _messageService.DeleteTopic(topic);
            return RedirectToAction("QueueInfo");
        }
        public ActionResult QueueInfo(string topic)
        {
            var topicQueueInfos = _messageService.GetTopicQueueInfo(topic);
            return View(new TopicQueueViewModel
            {
                Topic = topic,
                TopicQueueInfos = topicQueueInfos
            });
        }
        public ActionResult ConsumerInfo(string group, string topic)
        {
            var consumerInfos = _messageService.GetConsumerInfo(group, topic);
            return View(new ConsumerViewModel
            {
                Group = group,
                Topic = topic,
                ConsumerInfos = consumerInfos
            });
        }
        public ActionResult Message(string searchMessageId)
        {
            if (string.IsNullOrWhiteSpace(searchMessageId))
            {
                return View(new MessageViewModel());
            }
            var message = _messageService.GetMessageDetail(searchMessageId);
            var model = new MessageViewModel { SearchMessageId = searchMessageId };
            if (message != null)
            {
                model.MessageId = message.MessageId;
                model.Topic = message.Topic;
                model.QueueId = message.QueueId.ToString();
                model.QueueOffset = message.QueueOffset.ToString();
                model.Code = message.Code.ToString();
                model.Payload = Encoding.UTF8.GetString(message.Body);
                model.CreatedTime = message.CreatedTime.ToString();
                model.StoredTime = message.StoredTime.ToString();
            }
            return View(model);
        }
        public ActionResult AddQueue(string topic)
        {
            _messageService.AddQueue(topic);
            return RedirectToAction("QueueInfo");
        }
        public ActionResult DeleteQueue(string topic, int queueId)
        {
            _messageService.DeleteQueue(topic, queueId);
            return RedirectToAction("QueueInfo");
        }
        public ActionResult SetQueueProducerVisible(string topic, int queueId, bool visible)
        {
            _messageService.SetQueueProducerVisible(topic, queueId, visible);
            return RedirectToAction("QueueInfo");
        }
        public ActionResult SetQueueConsumerVisible(string topic, int queueId, bool visible)
        {
            _messageService.SetQueueConsumerVisible(topic, queueId, visible);
            return RedirectToAction("QueueInfo");
        }
    }
}