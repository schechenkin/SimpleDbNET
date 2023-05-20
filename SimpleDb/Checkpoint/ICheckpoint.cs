using System;

namespace SimpleDb.Checkpoint
{
    public interface ICheckpoint
    {
        void WaitCheckpointIsComplete();
        void Execute();
    }

    internal class Checkpoint : ICheckpoint
    {
        readonly ActiveRequestsCounter _activeRequestsCounter;
        readonly ISimpleDbServer _dbServer;
        object runningLockObj = new object();
        public volatile bool _isRunning;


        public Checkpoint(ActiveRequestsCounter activeRequestsCounter, ISimpleDbServer dbServer)
        {
            _activeRequestsCounter = activeRequestsCounter;
            _dbServer = dbServer;
        }
        
        public void Execute()
        {
            try
            {
                Monitor.Enter(runningLockObj);
                _isRunning = true;

                _activeRequestsCounter.WaitAllRequestsFinished();

                _dbServer.Log.Flush(true);
                _dbServer.GetBufferManager().FlushDirtyBuffers();
                _dbServer.ShrinkLogFile();

            }
            finally 
            {
                _isRunning = false; 
                Monitor.Exit(runningLockObj);
            }
        }

        public void WaitCheckpointIsComplete()
        {
            while(_isRunning)
            {
                //Monitor.Wait(runningLockObj, TimeSpan.FromSeconds(5));
                Thread.Yield();
            }    
        }
    }
}
