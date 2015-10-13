using System;
using System.Runtime.InteropServices;

namespace EQueue.Utils
{
    /// <summary>Service to catch console control events (ie CTRL-C) in C#.
    /// </summary>
    public class ConsoleEventHandlerService : IDisposable
    {
        public enum ConsoleEvent
        {
            CtrlC = 0,          //occurs when the user presses CTRL+C, Canbe disabled by set Console.TreatControlCAsInput to true, it is disabled by default.
            CtrlClose = 2,      //occurs when attempt is made to close the console
            CtrlLogoff = 5,     //occurs when the user is logging off
            CtrlShutdown = 6    //occurs when the system is being shutdown
        }

        /// <summary>Handler to be called when a console event occurs.
        /// </summary>
        public delegate void ControlEventHandler(int consoleEvent);

        private readonly ControlEventHandler _eventHandler;
        private ControlEventHandler _closingEventHandler;

        public ConsoleEventHandlerService()
        {
            _eventHandler = new ControlEventHandler(consoleEvent =>
            {
                if (IsCloseEvent(consoleEvent) && _closingEventHandler != null)
                {
                    _closingEventHandler(consoleEvent);
                }
            });
            SetConsoleCtrlHandler(_eventHandler, true);
        }
        ~ConsoleEventHandlerService()
        {
            Dispose(false);
        }

        public void RegisterClosingEventHandler(ControlEventHandler handler)
        {
            _closingEventHandler = handler;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private static bool IsCloseEvent(int consoleEvent)
        {
            if ((consoleEvent == (int)ConsoleEvent.CtrlC && !Console.TreatControlCAsInput)
                || consoleEvent == (int)ConsoleEvent.CtrlClose
                || consoleEvent == (int)ConsoleEvent.CtrlLogoff
                || consoleEvent == (int)ConsoleEvent.CtrlShutdown)
            {
                return true;
            }
            return false;
        }
        private void Dispose(bool disposing)
        {
            SetConsoleCtrlHandler(_eventHandler, false);
        }

        [DllImport("kernel32.dll")]
        static extern bool SetConsoleCtrlHandler(ControlEventHandler e, bool add);
    }
}
