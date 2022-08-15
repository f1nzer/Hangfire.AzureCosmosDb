using System;
using System.Threading;
using Hangfire.Azure.DistributedLock;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Hangfire.Server;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure;

#pragma warning disable 618
internal class ExpirationManager : IServerComponent
#pragma warning restore 618
{
	private const string DISTRIBUTED_LOCK_KEY = "locks:expiration:manager";
	private readonly DocumentTypes[] documents = { DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter, DocumentTypes.State };
	private readonly ILog logger = LogProvider.For<ExpirationManager>();
	private readonly CosmosDbStorage storage;
    private readonly DistributedLockExecutor distributedLockExecutor;

    public ExpirationManager(CosmosDbStorage storage)
	{
		this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        distributedLockExecutor = new DistributedLockExecutor(storage);
    }

	public void Execute(CancellationToken cancellationToken)
	{
		int expireOn = DateTime.UtcNow.ToEpoch();
        TimeSpan timeout = storage.StorageOptions.ExpirationCheckInterval;

        bool lockAcquired = distributedLockExecutor.TryInvoke(DISTRIBUTED_LOCK_KEY, timeout, () =>
        {
            // check if the token was cancelled
            cancellationToken.ThrowIfCancellationRequested();

            foreach (DocumentTypes type in documents)
            {
                cancellationToken.ThrowIfCancellationRequested();

                logger.Trace($"Removing outdated records from the [{type}] table.");

                string query = $"SELECT * FROM doc WHERE IS_DEFINED(doc.expire_on) AND doc.expire_on < {expireOn}";

                // remove only the aggregate counters when the type is Counter
                if (type == DocumentTypes.Counter) query += $" AND doc.counterType = {(int)CounterTypes.Aggregate}";

                int deleted = storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)type));

                logger.Trace($"Outdated [{deleted}] records removed from the [{type}] table.");
            }
        });

        if (!lockAcquired)
        {
            logger.Debug($@"An exception was thrown during acquiring distributed lock on the [{DISTRIBUTED_LOCK_KEY}] resource within [{timeout.TotalSeconds}] seconds. " +
                         $@"Outdated records were not removed. It will be retried in [{timeout.TotalSeconds}] seconds.");
        }

		// wait for the interval specified
		cancellationToken.WaitHandle.WaitOne(timeout);
	}
}