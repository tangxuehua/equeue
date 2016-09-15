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
            var brokerList = _messageService.GetClusterBrokers(clusterName);
            return View(new BrokerListViewModel
            {
                BrokerList = brokerList
            });
        }
        public ActionResult Message(string clusterName, string searchMessageId)
        {
            if (string.IsNullOrWhiteSpace(searchMessageId))
            {
                return View(new MessageViewModel());
            }
            var message = _messageService.GetMessageDetail(clusterName, searchMessageId);
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
    }
}