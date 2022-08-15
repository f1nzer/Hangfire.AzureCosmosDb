using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure.DistributedLock;

internal class DistributedLockExecutor
{
    private readonly CosmosDbStorage storage;
    private readonly ILog logger = LogProvider.For<DistributedLockExecutor>();

    public DistributedLockExecutor(CosmosDbStorage storage) =>
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));

    public bool TryInvoke(string resourceKey, TimeSpan timeout, Action action)
    {
        try
        {
            Invoke(resourceKey, timeout, action);
            return true;
        }
        catch (CosmosDbDistributedLockException ex) when (ex.Key == resourceKey)
        {
            return false;
        }
    }

    public void Invoke(string resourceKey, TimeSpan timeout, Action action)
    {
        using (AcquireLock(resourceKey, timeout))
        {
            action();
        }
    }

    public DistributedLockHandle AcquireLock(string resourceKey, TimeSpan timeout)
    {
        if (string.IsNullOrWhiteSpace(resourceKey))
        {
            throw new ArgumentNullException(nameof(resourceKey));
        }

        Stopwatch lockAcquiringStopwatch  = Stopwatch.StartNew();
        bool tryAcquireLock = true;
        
        while (tryAcquireLock)
        {
            try
            {
                DistributedLockHandle lockHandle = AcquireOnce(resourceKey, timeout);
                logger.Trace($"Acquired lock [{resourceKey}] in [{lockAcquiringStopwatch.Elapsed.TotalMilliseconds:#.##}] ms.");
                return lockHandle;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                logger.Trace($"Unable to create a lock [{resourceKey}]. Status - 409 Conflict. Lock already exists");
            }
            
            if (lockAcquiringStopwatch.ElapsedMilliseconds > timeout.TotalMilliseconds)
            {
                tryAcquireLock = false;
            }
            else
            {
                int msLeft = (int) (timeout.TotalMilliseconds - lockAcquiringStopwatch.ElapsedMilliseconds);
                int sleepDuration = Math.Min(msLeft, 1_000);

                if (sleepDuration > 0)
                {
                    Thread.Sleep(sleepDuration);
                }
                else
                {
                    tryAcquireLock = false;
                }
            }
        }
        
        throw new CosmosDbDistributedLockException(
            $"Could not place a lock [{resourceKey}]: Lock timeout reached [{timeout.TotalSeconds}] seconds.", resourceKey);
    }
    
    private DistributedLockHandle AcquireOnce(string resourceKey, TimeSpan timeout)
    {
        logger.Trace($"Trying to acquire lock [{resourceKey}]");

		// ttl for lock document
		// this is if the expiration manager was not able to remove the orphan lock in time.
        int ttl = (int) Math.Max(60, timeout.TotalSeconds * 1.5);

        Lock data = new()
        {
            Id = resourceKey,
            LastHeartBeat = DateTime.UtcNow,
            TimeToLive = ttl
        };
        
        Lock lockObj = storage.Container.CreateItemWithRetries(data, PartitionKeys.Lock);
        return new DistributedLockHandle(lockObj, storage.Container);
    }
}