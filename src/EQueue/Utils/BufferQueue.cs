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
        private Queue<TMessage> _inputQueue;
        private Queue<TMessage> _processQueue;
        private SpinWait _spinWait = default(SpinWait);
        private Worker _messageWorker;
        private Action<TMessage> _handleMessageAction;
        private readonly string _name;
        private readonly ILogger _logger;
        private volatile int _inputQueueSize;

        public BufferQueue(string name, int requestsWriteThreshold, Action<TMessage> handleMessageAction, ILogger logger)
        {
            _name = name;
            _requestsWriteThreshold = requestsWriteThreshold;
            _handleMessageAction = handleMessageAction;
            _inputQueue = new Queue<TMessage>();
            _processQueue = new Queue<TMessage>();
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
            lock (_lockObj)
            {
                _inputQueue.Enqueue(message);
                _inputQueueSize = _inputQueue.Count;
            }

            if (_inputQueueSize >= _requestsWriteThreshold)
            {
                Thread.Sleep(1);
            }
        }

        private void ProcessMessages()
        {
            if (_processQueue.Count == 0 && _inputQueueSize > 0)
            {
                SwapInputQueue();
            }
            if (_processQueue.Count > 0)
            {
                var count = _processQueue.Count;
                while (_processQueue.Count > 0)
                {
                    var message = _processQueue.Dequeue();
                    try
                    {
                        _handleMessageAction(message);
                    }
                    catch (Exception ex)
                    {
                        //TODO, Should we eat the exception here?
                        _logger.Error(_name + " process message has exception.", ex);
                        Thread.Sleep(1);
                    }
                }
            }
            else
            {
                _spinWait.SpinOnce();
            }
        }
        private void SwapInputQueue()
        {
            var tmp = _inputQueue;
            _inputQueue = _processQueue;
            _processQueue = tmp;
        }
    }
}
