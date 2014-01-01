using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using EQueue.Infrastructure;
using EQueue.Infrastructure.Extensions;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Scheduling;
using EQueue.Infrastructure.Socketing;
using EQueue.Remoting.Exceptions;

namespace EQueue.Remoting
{
    public class SocketRemotingClient : IRemotingClient
    {
        private readonly ClientSocket _clientSocket;
        private readonly ConcurrentDictionary<long, ResponseFuture> _responseFutureDict;
        private readonly Dictionary<int, IRequestProcessor> _requestProcessorDict;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        public SocketRemotingClient(string address, int port)
        {
            _clientSocket = new ClientSocket();
            _responseFutureDict = new ConcurrentDictionary<long, ResponseFuture>();
            _requestProcessorDict = new Dictionary<int, IRequestProcessor>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType());
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
        public RemotingResponse InvokeSync(string address, RemotingRequest request, int timeoutMillis)
        {
            var message = _binarySerializer.Serialize(request);
            var taskCompletionSource = new TaskCompletionSource<RemotingResponse>();
            var responseFuture = new ResponseFuture(request, timeoutMillis, taskCompletionSource);
            _responseFutureDict.TryAdd(request.Sequence, responseFuture);
            try
            {
                _clientSocket.SendMessage(message, x => responseFuture.SendRequestSuccess = true);
                var response = taskCompletionSource.Task.WaitResult<RemotingResponse>(timeoutMillis);
                if (response == null)
                {
                    if (responseFuture.SendRequestSuccess)
                    {
                        throw new RemotingTimeoutException(address, request, timeoutMillis);
                    }
                    else
                    {
                        throw new RemotingSendRequestException(address, request, responseFuture.SendException);
                    }
                }
                return response;
            }
            catch (Exception ex)
            {
                throw new RemotingSendRequestException(address, request, ex);
            }
        }
        public Task<RemotingResponse> InvokeAsync(string address, RemotingRequest request, int timeoutMillis)
        {
            var message = _binarySerializer.Serialize(request);
            var taskCompletionSource = new TaskCompletionSource<RemotingResponse>();
            var responseFuture = new ResponseFuture(request, timeoutMillis, taskCompletionSource);
            _responseFutureDict.TryAdd(request.Sequence, responseFuture);
            try
            {
                _clientSocket.SendMessage(message, sendResult =>
                {
                    responseFuture.SendRequestSuccess = sendResult.Success;
                    if (!sendResult.Success)
                    {
                        responseFuture.CompleteRequestTask(null);
                        _responseFutureDict.Remove(request.Sequence);
                        _logger.ErrorFormat("Send request [{0}] to channel <{1}> failed, exception:{2}", request, address, sendResult.Exception);
                    }
                });
            }
            catch (Exception ex)
            {
                throw new RemotingSendRequestException(address, request, ex);
            }

            return taskCompletionSource.Task;
        }
        public void InvokeOneway(string address, RemotingRequest request, int timeoutMillis)
        {
            request.IsOneway = true;
            try
            {
                var message = _binarySerializer.Serialize(request);
                _clientSocket.SendMessage(message, x => { });
            }
            catch (Exception ex)
            {
                throw new RemotingSendRequestException(address, request, ex);
            }
        }
        public void RegisterRequestProcessor(int requestCode, IRequestProcessor requestProcessor)
        {
            _requestProcessorDict[requestCode] = requestProcessor;
        }

        private void ProcessRemotingResponse(byte[] responseMessage)
        {
            var remotingResponse = _binarySerializer.Deserialize<RemotingResponse>(responseMessage);
            ResponseFuture responseFuture;
            if (_responseFutureDict.TryGetValue(remotingResponse.Sequence, out responseFuture))
            {
                responseFuture.CompleteRequestTask(remotingResponse);
            }
        }
        private void ScanTimeoutRequest()
        {
            var timeoutRequestList = new List<RemotingRequest>();
            foreach (var responseFuture in _responseFutureDict.Values)
            {
                if (responseFuture.IsTimeout())
                {
                    responseFuture.CompleteRequestTask(null);
                    timeoutRequestList.Add(responseFuture.Request);
                }
            }
            foreach (var request in timeoutRequestList)
            {
                _responseFutureDict.Remove(request.Sequence);
                _logger.WarnFormat("Removed timeout request:{0}", request);
            }
        }
    }
}
