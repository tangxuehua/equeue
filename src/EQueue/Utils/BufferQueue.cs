using System;
using System.Collections.Generic;
using System.Threading;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Utils
{
    public class BufferQueue<TMessage>
    {
        private readonly object _lockObj = new object();
        private int _requestsWriteThreshold;
        private IList<TMessage> _inputQueue;
        private IList<TMessage> _processQueue;
        private volatile bool _hasNewMessageNotified = false;
        private SpinWait _spinWait = default(SpinWait);
        private Worker _messageWorker;
        private Action<TMessage> _handleMessageAction;
        private readonly string _name;
        private readonly ILogger _logger;

        public BufferQueue(string name, int requestsWriteThreshold, Action<TMessage> handleMessageAction, ILogger logger)
        {
            _name = name;
            _requestsWriteThreshold = requestsWriteThreshold;
            _handleMessageAction = handleMessageAction;
            _inputQueue = new List<TMessage>();
            _processQueue = new List<TMessage>();
            _messageWorker = new Worker(name + ".ProcessMessages", ProcessMessages);
            _logger = logger;
        }

        public void Start()
        {
            _messageWorker.Start();
        }
        public void Stop()
        {
            _messageWorker.Stop();
        }
        public void EnqueueMessage(TMessage message)
        {
            var inputQueueSize = 0;

            lock (_lockObj)
            {
                _inputQueue.Add(message);
                inputQueueSize = _inputQueue.Count;
                if (!_hasNewMessageNotified)
                {
                    _hasNewMessageNotified = true;
                }
            }

            if (inputQueueSize >= _requestsWriteThreshold)
            {
                Thread.Sleep(1);
            }
        }

        private void ProcessMessages()
        {
            if (_hasNewMessageNotified)
            {
                lock (_lockObj)
                {
                    _hasNewMessageNotified = false;
                    SwapInputQueue();
                }
                var messageCount = _processQueue.Count;
                if (messageCount > 0)
                {
                    foreach (var message in _processQueue)
                    {
                        while (true)
                        {
                            try
                            {
                                _handleMessageAction(message);
                                break;
                            }
                            catch (Exception ex)
                            {
                                _logger.Error(_name + " process message has exception.", ex);
                                Thread.Sleep(1);
                            }
                        }
                    }
                    _processQueue.Clear();
                }
                return;
            }
            _spinWait.SpinOnce();
        }
        private void SwapInputQueue()
        {
            var tmp = _inputQueue;
            _inputQueue = _processQueue;
            _processQueue = tmp;
        }
    }
}
