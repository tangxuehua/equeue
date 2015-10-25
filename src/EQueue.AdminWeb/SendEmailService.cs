using System;
using System.Configuration;
using System.Net;
using System.Net.Mail;
using System.Text;
using ECommon.Logging;

namespace EQueue.AdminWeb
{
    public class SendEmailService
    {
        private SmtpClient _client;
        private readonly string _senderMail;
        private readonly string[] _targetEmails;
        private readonly ILogger _logger;
        private const string MailSubject = "EQueue消息堆积报警";
        private const string MailBodyFormat = "EQueue消息堆积已达到{0}，请尽快处理！";

        public SendEmailService(ILoggerFactory loggerFactory)
        {
            _client = new SmtpClient(ConfigurationManager.AppSettings["mailHost"]);
            _client.Credentials = new NetworkCredential(ConfigurationManager.AppSettings["mailUsername"], ConfigurationManager.AppSettings["mailpassword"]);
            _senderMail = ConfigurationManager.AppSettings["senderMail"];
            _targetEmails = ConfigurationManager.AppSettings["targetMails"].Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void SendTooManyMessageNotConsumedNotification(long unconsumedMessageCount)
        {
            try
            {
                var body = string.Format(MailBodyFormat, unconsumedMessageCount);
                var message = new MailMessage();
                message.From = new MailAddress(_senderMail);
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
                _logger.Error("SendTooManyMessageNotConsumedNotification failed.", ex);
            }
        }
    }
}