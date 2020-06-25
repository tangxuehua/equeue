using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EQueue.AdminWeb.Models;
using Microsoft.AspNetCore.Mvc;

namespace EQueue.AdminWeb.Controllers
{
    public class BrokerController : Controller
    {
        private MessageService _messageService;

        public BrokerController(MessageService messageService)
        {
            _messageService = messageService;
        }

        public async Task<ActionResult> Index(string clusterName, string brokerName)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var result = await _messageService.QueryBrokerStatisticInfo(clusterName, brokerName);
            return View(result);
        }
        public async Task<ActionResult> QueueInfoList(string clusterName, string brokerName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var topicQueueInfoList = await _messageService.GetTopicQueueInfoList(clusterName, brokerName, topic);
            return View(new BrokerTopicQueueListViewModel
            {
                Topic = topic,
                TopicQueueInfoList = topicQueueInfoList
            });
        }
        public async Task<ActionResult> ConsumeInfoList(string clusterName, string brokerName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var topicConsumeInfoList = await _messageService.GetTopicConsumeInfoList(clusterName, brokerName, group, topic);
            return View(new BrokerTopicConsumeListViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfoList = topicConsumeInfoList
            });
        }
        public async Task<ActionResult> ProducerList(string clusterName, string brokerName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var producerList = await _messageService.GetProducerInfoList(clusterName, brokerName);
            return View(new BrokerProducerListViewModel
            {
                ProducerList = producerList
            });
        }
        public async Task<ActionResult> ConsumerList(string clusterName, string brokerName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var consumerList = await _messageService.GetConsumerInfoList(clusterName, brokerName, group, topic);
            return View(new BrokerConsumerListViewModel
            {
                Group = group,
                Topic = topic,
                ConsumerList = consumerList
            });
        }
        public async Task<ActionResult> LatestSendMessages(string clusterName, string brokerName)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var messageList = await _messageService.GetLatestSendMessagesList(clusterName, brokerName);
            var messageInfoList = new List<MessageInfo>();

            foreach (var message in messageList)
            {
                string[] array = message.Split('_');
                var messageId = array[0];
                var createTime = new DateTime(long.Parse(array[1]));
                var storedTime = new DateTime(long.Parse(array[2]));
                messageInfoList.Add(new MessageInfo
                {
                    MessageId = messageId,
                    CreateTime = createTime,
                    StoredTime = storedTime
                });
            }
            messageInfoList.Sort((x, y) =>
            {
                if (x.StoredTime.Ticks > y.StoredTime.Ticks)
                {
                    return -1;
                }
                else if (x.StoredTime.Ticks < y.StoredTime.Ticks)
                {
                    return 1;
                }
                return 0;
            });

            var sequence = 1;
            foreach (var messageInfo in messageInfoList)
            {
                messageInfo.Sequence = sequence++;
            }
            return View(new BrokerLatestMessageIdListViewModel
            {
                MessageInfoList = messageInfoList
            });
        }
        public async Task<ActionResult> CreateTopic(string clusterName, string brokerName, string topic, int? initialQueueCount)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.CreateTopic(clusterName, brokerName, topic, initialQueueCount ?? 4);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, BrokerName = brokerName, Topic = topic });
        }
        public async Task<ActionResult> DeleteTopic(string clusterName, string brokerName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.DeleteTopic(clusterName, brokerName, topic);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, BrokerName = brokerName, Topic = topic });
        }
        public async Task<ActionResult> AddQueue(string clusterName, string brokerName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.AddQueue(clusterName, brokerName, topic);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, BrokerName = brokerName, Topic = topic });
        }
        public async Task<ActionResult> DeleteQueue(string clusterName, string brokerName, string topic, int queueId)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.DeleteQueue(clusterName, brokerName, topic, queueId);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, BrokerName = brokerName, Topic = topic });
        }
        public async Task<ActionResult> SetQueueProducerVisible(string clusterName, string brokerName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.SetQueueProducerVisible(clusterName, brokerName, topic, queueId, visible);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, BrokerName = brokerName, Topic = topic });
        }
        public async Task<ActionResult> SetQueueConsumerVisible(string clusterName, string brokerName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.SetQueueConsumerVisible(clusterName, brokerName, topic, queueId, visible);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, BrokerName = brokerName, Topic = topic });
        }
        [HttpGet]
        public ActionResult SetQueueNextConsumeOffset(string clusterName, string brokerName, string consumerGroup, string topic, int queueId)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var model = new SetQueueNextConsumeOffsetViewModel
            {
                ConsumerGroup = consumerGroup,
                Topic = topic,
                QueueId = queueId
            };
            return View(model);
        }
        [HttpPost]
        public async Task<ActionResult> SetQueueNextConsumeOffset(string clusterName, string brokerName, string consumerGroup, string topic, int queueId, long nextOffset)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.SetQueueNextConsumeOffset(clusterName, brokerName, consumerGroup, topic, queueId, nextOffset);
            return RedirectToAction("ConsumeInfoList", new { ClusterName = clusterName, BrokerName = brokerName, Group = consumerGroup, Topic = topic });
        }
        public async Task<ActionResult> DeleteConsumerGroup(string clusterName, string brokerName, string consumerGroup)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            await _messageService.DeleteConsumerGroup(clusterName, brokerName, consumerGroup);
            return RedirectToAction("ConsumeInfoList", new { ClusterName = clusterName, BrokerName = brokerName });
        }
    }
}