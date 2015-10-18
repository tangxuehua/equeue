using System.Text;
using System.Web.Mvc;
using EQueue.AdminWeb.Controls;
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
                UnConsumedQueueMessageCount = result.MinConsumedMessagePosition,
                CurrentMessageOffset = result.CurrentMessagePosition,
                PersistedMessageOffset = result.CurrentMessagePosition,
                UnPersistedMessageCount = result.CurrentMessagePosition - result.CurrentMessagePosition,
                MinMessageOffset = result.MinMessageOffset
            });
        }
        public ActionResult CreateTopic(string topic, int initialQueueCount)
        {
            _messageService.CreateTopic(topic, initialQueueCount);
            return RedirectToAction("TopicInfo");
        }
        public ActionResult TopicInfo(string topic)
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
        public ActionResult TopicConsumeInfo(string group, string topic)
        {
            var topicConsumeInfos = _messageService.GetTopicConsumeInfo(group, topic);
            return View(new TopicConsumeViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfos = topicConsumeInfos
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
                model.QueueId = message.QueueId.ToString();
                model.QueueOffset = message.QueueOffset.ToString();
                model.Code = message.Code.ToString();
                model.Content = Encoding.UTF8.GetString(message.Body);
                model.Topic = message.Topic;
                model.CreatedTime = message.CreatedTime.ToString();
                model.StoredTime = message.StoredTime.ToString();
            }
            return View(model);
        }
        public ActionResult AddQueue(string topic)
        {
            _messageService.AddQueue(topic);
            return RedirectToAction("TopicInfo");
        }
        public ActionResult RemoveQueue(string topic, int queueId)
        {
            _messageService.RemoveQueue(topic, queueId);
            return RedirectToAction("TopicInfo");
        }
        public ActionResult EnableQueue(string topic, int queueId)
        {
            _messageService.EnableQueue(topic, queueId);
            return RedirectToAction("TopicInfo");
        }
        public ActionResult DisableQueue(string topic, int queueId)
        {
            _messageService.DisableQueue(topic, queueId);
            return RedirectToAction("TopicInfo");
        }
    }
}