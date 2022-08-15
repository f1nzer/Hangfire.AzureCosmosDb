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

    public DistributedLockExecutor(CosmosDbStorage storage)
    {
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

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

        int attempt = 1;
        while (true)
        {
            try
            {
                return AcquireOnce(resourceKey, timeout);
            }
            catch (CosmosDbDistributedLockException ex) when (ex.Key == resourceKey)
            {
                if (attempt < 3)
                {
                    attempt++;
                    continue;
                }
                
                throw;
            }   
        }
    }
    
    private DistributedLockHandle AcquireOnce(string resourceKey, TimeSpan timeout)
    {
        logger.Trace($"Trying to acquire lock [{resourceKey}] within [{timeout.TotalSeconds}] seconds");

		Stopwatch acquireStart = new();
		acquireStart.Start();

		// ttl for lock document
		// this is if the expiration manager was not able to remove the orphan lock in time.
        int ttl = (int) Math.Max(60, timeout.TotalSeconds * 1.5);

        Lock? @lock = null;
		do
		{
			Lock data = new()
			{
				Id = resourceKey,
				LastHeartBeat = DateTime.UtcNow,
				TimeToLive = ttl
			};

			try
			{
				@lock = storage.Container.CreateItemWithRetries(data, PartitionKeys.Lock);
				break;
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
			{
				logger.Trace($"Unable to create a lock [{resourceKey}]. Status - 409 Conflict. Lock already exists");
			}
			catch (Exception ex)
			{
				logger.ErrorException($"Unable to create a lock [{resourceKey}]", ex);
			}
            
            // TODO: count elapsed time on top level

			// check the timeout
			if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
            {
                throw new CosmosDbDistributedLockException(
                    $"Could not place a lock [{resourceKey}]: Lock timeout reached [{timeout.TotalSeconds}] seconds.", resourceKey);
            }

			logger.Trace($"Unable to acquire lock [{resourceKey}]. Will try after [2] seconds");
			Thread.Sleep(2000);

		} while (@lock == null);

		logger.Trace($"Acquired lock [{resourceKey}] for [{timeout.TotalSeconds}] seconds; in [{acquireStart.Elapsed.TotalMilliseconds:#.##}] ms.");
        return new DistributedLockHandle(@lock, storage.Container);
    }
}