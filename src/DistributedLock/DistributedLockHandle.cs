using System;
using System.Net;
using System.Threading;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure.DistributedLock;

internal class DistributedLockHandle : IDisposable
{
	private readonly ILog logger = LogProvider.For<DistributedLockHandle>();
	private readonly object syncLock = new();
    private readonly Timer? timer;
    private readonly Container container;

    private bool disposed;
    private Lock lockObj;

    public DistributedLockHandle(Lock lockObj, Container container)
    {
        this.lockObj = lockObj ?? throw new ArgumentNullException(nameof(lockObj));
        this.container = container ?? throw new ArgumentNullException(nameof(container));

        if (this.lockObj.TimeToLive != null)
        {
            // set the timer for the KeepLockAlive callbacks
            timer = new Timer(KeepLockAlive, this.lockObj, GetTtlDueTime(), Timeout.InfiniteTimeSpan);   
        }
	}

    private TimeSpan GetTtlDueTime()
    {
        if (lockObj.TimeToLive == null)
        {
            throw new InvalidOperationException("TTL value should be not null for ");
        }
        
        TimeSpan temp = TimeSpan.FromSeconds(this.lockObj.TimeToLive.Value);
        TimeSpan dueTime = new(temp.Ticks / 2);
        return dueTime.TotalSeconds < 1 ? TimeSpan.FromSeconds(1) : dueTime;
    }

	public void Dispose()
	{
		if (disposed) return;
		disposed = true;

		lock (syncLock)
		{
			try
			{
				container.DeleteItemWithRetries<Lock>(lockObj.Id, PartitionKeys.Lock);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				logger.Trace($"Unable to release the lock [{lockObj.Id}]. Status - 404 NotFound");
			}
			catch (Exception exception)
			{
				logger.ErrorException($"Unable to release the lock [{lockObj.Id}]", exception);
			}
			finally
			{
				timer?.Dispose();
				logger.Trace($"Lock [{lockObj.Id}] is released");
			}
		}
	}

	/// <summary>
	///     this is to update the document so that the ttl gets reset and does not removes the document pre-maturely
	/// </summary>
	// ReSharper disable once MemberCanBePrivate.Global
	internal void KeepLockAlive(object data)
	{
		if (disposed) return;
		lock (syncLock)
		{
			if (data is not Lock temp) return;

			try
			{
				logger.Trace($"Preparing the keep-alive query for lock: [{temp.Id}]");

				PatchItemRequestOptions patchItemRequestOptions = new() { IfMatchEtag = temp.ETag };
				PatchOperation[] patchOperations =
				{
					PatchOperation.Set("/last_heartbeat", DateTime.UtcNow.ToEpoch())
				};

				lockObj = container.PatchItemWithRetries<Lock>(temp.Id, PartitionKeys.Lock, patchOperations, patchItemRequestOptions);
            }
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				logger.Trace($"Lock [{temp.Id}] keep-alive query failed. Status - 404 NotFound. Keep-alive query won't be executed anymore");
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
				logger.Trace($"Lock [{temp.Id}] keep-alive query failed. Most likely the lock was updated by some other server. Status - 412 PreconditionFailed. Keep-alive query won't be executed anymore");
			}
			catch (Exception ex)
			{
				logger.DebugException($"Unable to execute keep-alive query for the lock [{temp.Id}]", ex);
			}
            
            // set the time for the next callback
            timer?.Change(GetTtlDueTime(), Timeout.InfiniteTimeSpan);
            logger.Trace($"Keep-alive query for lock: [{temp.Id}] sent");
		}
	}
}