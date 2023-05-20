namespace SimpleDb.Checkpoint
{
    public class ActiveRequestsCounter
    {
        long counter;
        
        public void Increment()
        {
            Interlocked.Increment(ref counter);
        }

        public void Decrement() 
        { 
            Interlocked.Decrement(ref counter);
        }

        public void WaitAllRequestsFinished()
        {
            while(Interlocked.Read(ref counter) > 0)
                Thread.Yield();
        }
    }
}
