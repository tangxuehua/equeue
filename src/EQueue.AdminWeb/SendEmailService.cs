using System;
using System.Net;
using System.Net.Mail;
using System.Text;
using ECommon.Logging;
using EQueue.Protocols.NameServers;
using Microsoft.Extensions.Configuration;

namespace EQueue.AdminWeb
{
    public class SendEmailService
    {
        private readonly SmtpClient _client;
        private readonly string _senderMail;
        private readonly string[] _targetEmails;
        private readonly ILogger _logger;
        private const string MailSubject = "EQueue消息堆积报警";
        private const string MailBodyFormat = "您好，您订阅的Topic已出现消息堆积，请尽快处理，堆积详情如下：<br/>Topic：{0}<br/>消费者分组：{1}<br/>堆积数：{2}<br/>在线消费者个数：{3}<br/>消费吞吐：{4}<br/>总队列数：{5}";

        public SendEmailService(IConfiguration configuration, ILoggerFactory loggerFactory)
        {
            var mailSetting = new MailSetting();
            configuration.GetSection("MailSetting").Bind(mailSetting);
            _client = new SmtpClient(mailSetting.Host)
            {
                Credentials = new NetworkCredential(mailSetting.Username, mailSetting.Password)
            };
            _senderMail = mailSetting.SenderMail;
            _targetEmails = mailSetting.TargetMails.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void SendMessageAccumulateNotification(TopicAccumulateInfo topicAccumulateInfo)
        {
            try
            {
                var body = string.Format(MailBodyFormat,
                    topicAccumulateInfo.Topic,
                    topicAccumulateInfo.ConsumerGroup,
                    topicAccumulateInfo.AccumulateCount,
                    topicAccumulateInfo.OnlineConsumerCount,
                    topicAccumulateInfo.ConsumeThroughput,
                    topicAccumulateInfo.QueueCount);
                var message = new MailMessage
                {
                    From = new MailAddress(_senderMail)
                };
                foreach (var targetMail in _targetEmails)
                {
                    message.To.Add(targetMail);
                }
                message.Subject = MailSubject;
                message.Body = body;
                message.SubjectEncoding = Encoding.UTF8;
                message.BodyEncoding = Encoding.UTF8;
                message.Priority = MailPriority.High;
                message.IsBodyHtml = true;
                _client.Send(message);
            }
            catch (Exception ex)
            {
                _logger.Error("SendMessageAccumulateNotification has exception.", ex);
            }
        }
    }

    public class MailSetting
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string SenderMail { get; set; }
        public string TargetMails { get; set; }
    }
}