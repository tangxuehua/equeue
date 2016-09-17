using System.Text;
using System.Web.Mvc;
using EQueue.AdminWeb.Models;

namespace EQueue.AdminWeb.Controllers
{
    [HandleError]
    public class HomeController : Controller
    {
        private MessageService _messageService;

        public HomeController(MessageService messageService)
        {
            _messageService = messageService;
        }

        public ActionResult Index()
        {
            var clusterList = _messageService.GetAllClusters();
            return View(new ClusterListViewModel
            {
                ClusterList = clusterList
            });
        }
        public ActionResult BrokerList(string clusterName)
        {
            ViewBag.ClusterName = clusterName;
            var brokerList = _messageService.GetClusterBrokers(clusterName);
            return View(new ClusterBrokerListViewModel
            {
                BrokerList = brokerList
            });
        }
        public ActionResult QueueInfoList(string clusterName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var topicQueueInfoList = _messageService.GetTopicQueueInfoList(clusterName, topic);
            return View(new ClusterTopicQueueListViewModel
            {
                Topic = topic,
                TopicQueueInfoList = topicQueueInfoList
            });
        }
        public ActionResult ConsumeInfoList(string clusterName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var topicConsumeInfoList = _messageService.GetTopicConsumeInfoList(clusterName, group, topic);
            return View(new ClusterTopicConsumeListViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfoList = topicConsumeInfoList
            });
        }
        public ActionResult ProducerList(string clusterName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var producerList = _messageService.GetProducerInfoList(clusterName);
            return View(new ClusterProducerListViewModel
            {
                ProducerList = producerList
            });
        }
        public ActionResult ConsumerList(string clusterName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var consumerList = _messageService.GetConsumerInfoList(clusterName, group, topic);
            return View(new ClusterConsumerListViewModel
            {
                Group = group,
                Topic = topic,
                ConsumerList = consumerList
            });
        }
        public ActionResult Message(string clusterName, string searchMessageId)
        {
            ViewBag.ClusterName = clusterName;
            if (string.IsNullOrWhiteSpace(searchMessageId))
            {
                return View(new MessageViewModel());
            }
            var message = _messageService.GetMessageDetail(clusterName, searchMessageId);
            var model = new MessageViewModel { ClusterName = clusterName, SearchMessageId = searchMessageId };
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
    }
}