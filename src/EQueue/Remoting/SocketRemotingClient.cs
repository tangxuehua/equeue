using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using EQueue.Infrastructure.Extensions;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Scheduling;
using EQueue.Infrastructure.Socketing;
using EQueue.Remoting.Exceptions;

namespace EQueue.Remoting
{
    public class SocketRemotingClient
    {
        private readonly string _address;
        private readonly int _port;
        private readonly ClientSocket _clientSocket;
        private readonly ConcurrentDictionary<long, ResponseFuture> _responseFutureDict;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        public SocketRemotingClient(string address = "127.0.0.1", int port = 5000)
        {
            _address = address;
            _port = port;
            _clientSocket = new ClientSocket();
            _responseFutureDict = new ConcurrentDictionary<long, ResponseFuture>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            _clientSocket.Connect(address, port);
        }

        public void Start()
        {
            _clientSocket.Start(ProcessRemotingResponse);
            _scheduleService.ScheduleTask(ScanTimeoutRequest, 1000 * 3, 1000);
        }
        public void Shutdown()
        {
            _clientSocket.Shutdown();
        }
        public RemotingResponse InvokeSync(RemotingRequest request, int timeoutMillis)
        {
            var message = RemotingUtil.BuildRequestMessage(request);
            var taskCompletionSource = new TaskCompletionSource<RemotingResponse>();
            var responseFuture = new ResponseFuture(request, timeoutMillis, taskCompletionSource);
            var response = default(RemotingResponse);

            if (!_responseFutureDict.TryAdd(request.Sequence, responseFuture))
            {
                throw new Exception(string.Format("Try to add response future failed. request sequence:{0}", request.Sequence));
            }
            try
            {
                _clientSocket.SendMessage(message, sendResult => SendMessageCallback(responseFuture, request, _address, sendResult));
                response = taskCompletionSource.Task.WaitResult<RemotingResponse>(timeoutMillis);
            }
            catch (Exception ex)
            {
                throw new RemotingSendRequestException(_address, request, ex);
            }

            if (response == null)
            {
                if (responseFuture.SendRequestSuccess)
                {
                    throw new RemotingTimeoutException(_address, request, timeoutMillis);
                }
                else
                {
                    throw new RemotingSendRequestException(_address, request, responseFuture.SendException);
                }
            }
            return response;
        }
        public Task<RemotingResponse> InvokeAsync(RemotingRequest request, int timeoutMillis)
        {
            var message = RemotingUtil.BuildRequestMessage(request);
            var taskCompletionSource = new TaskCompletionSource<RemotingResponse>();
            var responseFuture = new ResponseFuture(request, timeoutMillis, taskCompletionSource);

            if (!_responseFutureDict.TryAdd(request.Sequence, responseFuture))
            {
                throw new Exception(string.Format("Try to add response future failed. request sequence:{0}", request.Sequence));
            }
            try
            {
                _clientSocket.SendMessage(message, sendResult => SendMessageCallback(responseFuture, request, _address, sendResult));
            }
            catch (Exception ex)
            {
                throw new RemotingSendRequestException(_address, request, ex);
            }

            return taskCompletionSource.Task;
        }
        public void InvokeOneway(RemotingRequest request, int timeoutMillis)
        {
            request.IsOneway = true;
            var message = RemotingUtil.BuildRequestMessage(request);
            try
            {
                _clientSocket.SendMessage(message, x => { });
            }
            catch (Exception ex)
            {
                throw new RemotingSendRequestException(_address, request, ex);
            }
        }

        private void ProcessRemotingResponse(byte[] responseMessage)
        {
            Task.Factory.StartNew(() =>
            {
                var remotingResponse = RemotingUtil.ParseResponse(responseMessage);
                ResponseFuture responseFuture;
                if (_responseFutureDict.TryRemove(remotingResponse.Sequence, out responseFuture))
                {
                    responseFuture.CompleteRequestTask(remotingResponse);
                }
                else
                {
                    _logger.ErrorFormat("Remoting response returned, but the responseFuture was removed already. request sequence:{0}", remotingResponse.Sequence);
                }
            });
        }
        private void ScanTimeoutRequest()
        {
            var timeoutResponseFutureKeyList = new List<long>();
            foreach (var entry in _responseFutureDict)
            {
                if (entry.Value.IsTimeout())
                {
                    timeoutResponseFutureKeyList.Add(entry.Key);
                }
            }
            foreach (var key in timeoutResponseFutureKeyList)
            {
                ResponseFuture responseFuture;
                if (_responseFutureDict.TryRemove(key, out responseFuture))
                {
                    responseFuture.CompleteRequestTask(null);
                    _logger.WarnFormat("Removed timeout request:{0}", responseFuture.Request);
                }
            }
        }
        private void SendMessageCallback(ResponseFuture responseFuture, RemotingRequest request, string address, SendResult sendResult)
        {
            responseFuture.SendRequestSuccess = sendResult.Success;
            responseFuture.SendException = sendResult.Exception;
            if (!sendResult.Success)
            {
                responseFuture.CompleteRequestTask(null);
                _responseFutureDict.Remove(request.Sequence);
                _logger.ErrorFormat("Send request {0} to channel <{1}> failed, exception:{2}", request, address, sendResult.Exception);
            }
        }
    }
}
