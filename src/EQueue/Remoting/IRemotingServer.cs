namespace EQueue.Remoting
{
    public interface IRemotingServer
    {
        void Start();
        void Shutdown();
        void RegisterRequestProcessor(int requestCode, IRequestProcessor requestProcessor);
    }
}
