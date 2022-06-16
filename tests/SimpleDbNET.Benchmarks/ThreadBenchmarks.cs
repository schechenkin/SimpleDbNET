using BenchmarkDotNet.Attributes;
using Proto.Utilities.Benchmark;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDbNET.Benchmarks
{
    [MemoryDiagnoser(false)]
    public class ThreadBenchmarks
    {
        [Params(-1, 2)]
        public int MaxConcurrency { get; set; }

        private BenchmarkThreadHelper threadHelper;
        private ParallelOptions parallelOptions;

        private readonly object locker = new();
        private int counter;

        private readonly Action interlockedAction;
        private readonly Action lockedAction;
        private readonly Action overheadAction = () => { };

        public ThreadBenchmarks()
        {
            interlockedAction = () =>
            {
                Interlocked.Increment(ref counter);
            };
            lockedAction = () =>
            {
                unchecked
                {
                    lock (locker)
                    {
                        ++counter;
                    }
                }
            };
        }


        [GlobalSetup(Targets = new[] { nameof(ParallelOverhead), nameof(ParallelInterlocked), nameof(ParallelLocked) })]
        public void SetupParallelOptions()
        {
            parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = MaxConcurrency };
        }

        [Benchmark]
        public void ParallelOverhead()
        {
            Parallel.Invoke(parallelOptions,
                overheadAction,
                overheadAction,
                overheadAction,
                overheadAction);
        }

        [Benchmark]
        public void ParallelInterlocked()
        {
            Parallel.Invoke(parallelOptions,
                interlockedAction,
                interlockedAction,
                interlockedAction,
                interlockedAction);
        }

        [Benchmark]
        public void ParallelLocked()
        {
            Parallel.Invoke(parallelOptions,
                lockedAction,
                lockedAction,
                lockedAction,
                lockedAction);
        }

        [GlobalSetup(Target = nameof(ThreadHelperOverhead))]
        public void SetupOverhead()
        {
            threadHelper = new BenchmarkThreadHelper();
            threadHelper.AddAction(overheadAction);
            threadHelper.AddAction(overheadAction);
            threadHelper.AddAction(overheadAction);
            threadHelper.AddAction(overheadAction);
        }

        [Benchmark]
        public void ThreadHelperOverhead()
        {
            threadHelper.ExecuteAndWait();
        }

        [GlobalSetup(Target = nameof(ThreadHelperInterlocked))]
        public void SetupInterlocked()
        {
            threadHelper = new BenchmarkThreadHelper();
            threadHelper.AddAction(interlockedAction);
            threadHelper.AddAction(interlockedAction);
            threadHelper.AddAction(interlockedAction);
            threadHelper.AddAction(interlockedAction);
        }

        [Benchmark]
        public void ThreadHelperInterlocked()
        {
            threadHelper.ExecuteAndWait();
        }

        [GlobalSetup(Target = nameof(ThreadHelperLocked))]
        public void SetupLocked()
        {
            threadHelper = new BenchmarkThreadHelper();
            threadHelper.AddAction(lockedAction);
            threadHelper.AddAction(lockedAction);
            threadHelper.AddAction(lockedAction);
            threadHelper.AddAction(lockedAction);
        }

        [Benchmark]
        public void ThreadHelperLocked()
        {
            threadHelper.ExecuteAndWait();
        }
    }
}
