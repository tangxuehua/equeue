using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using ECommon.Components;
using ECommon.Dapper;
using ECommon.Logging;

namespace EQueue.Broker
{
    public class SqlServerQueueStore : IQueueStore
    {
        private readonly SqlServerQueueStoreSetting _setting;
        private readonly ILogger _logger;

        public SqlServerQueueStore(SqlServerQueueStoreSetting setting)
        {
            _setting = setting;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public IEnumerable<Queue> LoadAllQueues()
        {
            _logger.Info("Start to load queues from db.");
            using (var connection = GetConnection())
            {
                var queues = new List<Queue>();
                var result = connection.QueryList(null, _setting.QueueTable);
                if (result != null)
                {
                    foreach (var item in result)
                    {
                        var topic = item.Topic as string;
                        var queueId = (int)item.QueueId;
                        var status = (int)item.Status;
                        var queue = new Queue(topic, queueId);
                        if (status == (int)QueueStatus.Normal)
                        {
                            queue.Enable();
                        }
                        else if (status == (int)QueueStatus.Disabled)
                        {
                            queue.Disable();
                        }
                        queues.Add(queue);
                    }
                }
                _logger.InfoFormat("{0} queues loaded from db.", queues.Count);
                return queues;
            }
        }
        public bool IsQueueExist(string topic, int queueId)
        {
            using (var connection = GetConnection())
            {
                return connection.GetCount(new
                {
                    Topic = topic,
                    QueueId = queueId
                }, _setting.QueueTable) > 0;
            }
        }
        public Queue GetQueue(string topic, int queueId)
        {
            using (var connection = GetConnection())
            {
                var result = connection.QueryList(new
                {
                    Topic = topic,
                    QueueId = queueId
                }, _setting.QueueTable).SingleOrDefault();
                if (result != null)
                {
                    var queue = new Queue(topic, queueId);
                    var status = (int)result.Status;
                    if (status == (int)QueueStatus.Normal)
                    {
                        queue.Enable();
                    }
                    else if (status == (int)QueueStatus.Disabled)
                    {
                        queue.Disable();
                    }
                    return queue;
                }
                return null;
            }
        }
        public void CreateQueue(Queue queue)
        {
            using (var connection = GetConnection())
            {
                var current = DateTime.Now;
                connection.Insert(new
                {
                    Topic = queue.Topic,
                    QueueId = queue.QueueId,
                    Status = queue.Status,
                    CreatedTime = current,
                    UpdatedTime = current
                }, _setting.QueueTable);
                _logger.InfoFormat("Create queue success, topic={0}, queueId={1}", queue.Topic, queue.QueueId);
            }
        }
        public void DeleteQueue(Queue queue)
        {
            using (var connection = GetConnection())
            {
                var count = connection.Delete(new
                {
                    Topic = queue.Topic,
                    QueueId = queue.QueueId
                }, _setting.QueueTable);
                if (count > 0)
                {
                    _logger.InfoFormat("Delete queue success, topic={0}, queueId={1}", queue.Topic, queue.QueueId);
                }
            }
        }
        public void UpdateQueue(Queue queue)
        {
            using (var connection = GetConnection())
            {
                var count = connection.Update(new
                {
                    Status = queue.Status,
                    UpdatedTime = DateTime.Now
                }, new
                {
                    Topic = queue.Topic,
                    QueueId = queue.QueueId
                }, _setting.QueueTable);
                if (count > 0)
                {
                    _logger.InfoFormat("Update queue success, topic={0}, queueId={1}, status={2}", queue.Topic, queue.QueueId, queue.Status);
                }
            }
        }

        private SqlConnection GetConnection()
        {
            return new SqlConnection(_setting.ConnectionString);
        }
    }
}
