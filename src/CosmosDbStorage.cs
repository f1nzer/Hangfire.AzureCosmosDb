﻿using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Hangfire.Azure;

/// <summary>
///     CosmosDbStorage extend the storage option for Hangfire.
/// </summary>
public sealed class CosmosDbStorage : JobStorage
{
    private readonly string containerName;
    private readonly string databaseName;
    private readonly ILog logger = LogProvider.For<CosmosDbStorage>();

    private readonly JsonSerializerSettings settings = new ()
    {
        NullValueHandling = NullValueHandling.Ignore,
        DateTimeZoneHandling = DateTimeZoneHandling.Utc,
        ContractResolver = new CamelCasePropertyNamesContractResolver
        {
            NamingStrategy = new CamelCaseNamingStrategy(false, false)
        }
    };

    /// <summary>
    ///     Creates an instance of CosmosDbStorage
    /// </summary>
    /// <param name="url">The url string to Cosmos Database</param>
    /// <param name="authSecret">The secret key for the Cosmos Database</param>
    /// <param name="databaseName">The name of the database to connect with</param>
    /// <param name="containerName">The name of the collection/container on the database</param>
    /// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
    internal CosmosDbStorage(
        string url,
        string authSecret,
        string databaseName,
        string containerName,
        CosmosDbStorageOptions? storageOptions = null)
        : this(databaseName, containerName, storageOptions)
    {
        if (string.IsNullOrEmpty(url))
        {
            throw new ArgumentNullException(nameof(url));
        }

        if (string.IsNullOrEmpty(authSecret))
        {
            throw new ArgumentNullException(nameof(authSecret));
        }

        CosmosClientBuilder builder = new(url, authSecret);
        ConfigureCosmosClientBuilder(builder);
        Client = builder.Build();
    }

    internal CosmosDbStorage(
        CosmosClientBuilder cosmosClientBuilder,
        string databaseName,
        string containerName,
        CosmosDbStorageOptions? storageOptions = null)
        : this(databaseName, containerName, storageOptions)
    {
        if (cosmosClientBuilder == null)
        {
            throw new ArgumentNullException(nameof(cosmosClientBuilder));
        }
        
        ConfigureCosmosClientBuilder(cosmosClientBuilder);
        Client = cosmosClientBuilder.Build();
    }

    private CosmosDbStorage(string databaseName, string containerName, CosmosDbStorageOptions? storageOptions = null)
    {
        if (string.IsNullOrEmpty(databaseName))
        {
            throw new ArgumentNullException(nameof(databaseName));
        }

        if (string.IsNullOrEmpty(containerName))
        {
            throw new ArgumentNullException(nameof(containerName));
        }

        this.databaseName = databaseName;
        this.containerName = containerName;
        StorageOptions = storageOptions ?? new CosmosDbStorageOptions();

        JobQueueProvider provider = new (this);
        QueueProviders = new PersistentJobQueueProviderCollection(provider);
    }

    internal PersistentJobQueueProviderCollection QueueProviders { get; }

    internal CosmosDbStorageOptions StorageOptions { get; set; }

    private CosmosClient Client { get; } = null!;

    internal Container Container { get; private set; } = null!;

    private void ConfigureCosmosClientBuilder(CosmosClientBuilder builder) => builder
        .WithApplicationName("Hangfire")
        .WithCustomSerializer(new CosmosJsonSerializer(settings))
        .WithThrottlingRetryOptions(TimeSpan.FromSeconds(30), 9);

    /// <summary>
    /// </summary>
    /// <returns></returns>
    public override IStorageConnection GetConnection() => new CosmosDbConnection(this);

    /// <summary>
    /// </summary>
    /// <returns></returns>
    public override IMonitoringApi GetMonitoringApi() => new CosmosDbMonitoringApi(this);

#pragma warning disable 618
    /// <summary>
    /// </summary>
    /// <returns></returns>
    public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
    {
        yield return new ExpirationManager(this);
        yield return new CountersAggregator(this);
    }

    /// <summary>
    ///     Prints out the storage options
    /// </summary>
    /// <param name="log"></param>
    public override void WriteOptionsToLog(ILog log)
    {
        StringBuilder info = new ();
        info.AppendLine("Using the following options for Azure Cosmos DB job storage:");
        info.AppendLine($"	Cosmos DB Url: [{Client.Endpoint.AbsoluteUri}]");
        info.AppendLine($"	Database: [{databaseName}]");
        info.AppendLine($"	Container: [{containerName}]");
        info.AppendLine($"	Request Timeout: [{Client.ClientOptions.RequestTimeout}]");
        info.AppendLine($"	Connection Mode: [{Client.ClientOptions.ConnectionMode}]");
        info.AppendLine($"	Region: [{Client.ClientOptions.ApplicationRegion}]");
        info.AppendLine($"	Max Retry Attempts On Rate Limited Requests: [{Client.ClientOptions.MaxRetryAttemptsOnRateLimitedRequests}]");
        info.AppendLine($"	Max Retry Wait Time On Rate Limited Requests: [{Client.ClientOptions.MaxRetryWaitTimeOnRateLimitedRequests!.Value}]");
        info.AppendLine($"	Create Storage If Not Exists: [{StorageOptions.CreateIfNotExists}]");
        info.AppendLine($"	Counter Aggregator Max Items: [{StorageOptions.CountersAggregateMaxItemCount}]");
        info.AppendLine($"	Transactional Lock Timeout: [{StorageOptions.TransactionalLockTimeout}]");
        info.AppendLine($"	Counter Aggregate Interval: [{StorageOptions.CountersAggregateInterval}]");
        info.AppendLine($"	Queue Poll Interval: [{StorageOptions.QueuePollInterval}]");
        info.AppendLine($"	Expiration Check Interval: [{StorageOptions.ExpirationCheckInterval}]");
        info.Append($"	Job Keep-Alive Interval: [{StorageOptions.JobKeepAliveInterval}]");
        log.Info(info.ToString);
    }

    /// <summary>
    ///     Return the name of the database
    /// </summary>
    /// <returns></returns>
    public override string ToString() => $"Cosmos DB : {databaseName}/{containerName}";

    /// <summary>
    ///     Creates and returns an instance of CosmosDbStorage
    /// </summary>
    /// <param name="url">The url string to Cosmos Database</param>
    /// <param name="authSecret">The secret key for the Cosmos Database</param>
    /// <param name="databaseName">The name of the database to connect with</param>
    /// <param name="containerName">The name of the collection/container on the database</param>
    /// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
    public static CosmosDbStorage Create(
        string url,
        string authSecret,
        string databaseName,
        string containerName,
        CosmosDbStorageOptions? storageOptions = null)
    {
        CosmosDbStorage storage = new (url, authSecret, databaseName, containerName, storageOptions);
        storage.InitializeAsync().ExecuteSynchronously();
        return storage;
    }


    /// <summary>
    ///     Creates and returns an instance of CosmosDbStorage
    /// </summary>
    /// <param name="cosmosClientBuilder">An instance of CosmosClientBuilder</param>
    /// <param name="databaseName">The name of the database to connect with</param>
    /// <param name="containerName">The name of the collection/container on the database</param>
    /// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
    public static CosmosDbStorage Create(
        CosmosClientBuilder cosmosClientBuilder,
        string databaseName,
        string containerName,
        CosmosDbStorageOptions? storageOptions = null)
    {
        CosmosDbStorage storage = new (cosmosClientBuilder, databaseName, containerName, storageOptions);
        storage.InitializeAsync().ExecuteSynchronously();
        return storage;
    }

    /// <summary>
    ///     Creates and returns an instance of CosmosDbStorage
    /// </summary>
    /// <param name="url">The url string to Cosmos Database</param>
    /// <param name="authSecret">The secret key for the Cosmos Database</param>
    /// <param name="databaseName">The name of the database to connect with</param>
    /// <param name="containerName">The name of the collection/container on the database</param>
    /// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
    /// <param name="cancellationToken">A cancellation token</param>
    public static async Task<CosmosDbStorage> CreateAsync(
        string url,
        string authSecret,
        string databaseName,
        string containerName,
        CosmosDbStorageOptions? storageOptions = null,
        CancellationToken cancellationToken = default)
    {
        CosmosDbStorage storage = new (url, authSecret, databaseName, containerName, storageOptions);
        await storage.InitializeAsync(cancellationToken);
        return storage;
    }

    /// <summary>
    ///     Creates and returns an instance of CosmosDbStorage
    /// </summary>
    /// <param name="cosmosClientBuilder">An instance of CosmosClientBuilder</param>
    /// <param name="databaseName">The name of the database to connect with</param>
    /// <param name="containerName">The name of the collection/container on the database</param>
    /// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
    /// <param name="cancellationToken">A cancellation token</param>
    public static async Task<CosmosDbStorage> CreateAsync(
        CosmosClientBuilder cosmosClientBuilder,
        string databaseName,
        string containerName,
        CosmosDbStorageOptions? storageOptions = null,
        CancellationToken cancellationToken = default)
    {
        CosmosDbStorage storage = new (cosmosClientBuilder, databaseName, containerName, storageOptions);
        await storage.InitializeAsync(cancellationToken);
        return storage;
    }

    private async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (!StorageOptions.CreateIfNotExists)
        {
            // check if container exists within database
            try
            {
                Container = await Client.GetContainer(databaseName, containerName).ReadContainerAsync(cancellationToken: cancellationToken);
                return;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException($"Cannot find an existing container named {containerName} within database {databaseName}");
            }
        }
        
        // create database
        logger.Info($"Creating database : [{databaseName}]");
        DatabaseResponse databaseResponse = await Client.CreateDatabaseIfNotExistsAsync(databaseName, cancellationToken: cancellationToken);

        // create container
        logger.Info($"Creating container : [{containerName}]");
        Database resultDatabase = databaseResponse.Database;

        ContainerProperties properties = new ()
        {
            Id = containerName,
            DefaultTimeToLive = -1,
            PartitionKeyPath = "/type",
            PartitionKeyDefinitionVersion = PartitionKeyDefinitionVersion.V2
        };

        // add the index policy
        Collection<CompositePath> compositeIndexes = new ()
        {
            new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
            new CompositePath { Path = "/created_on", Order = CompositePathSortOrder.Ascending }
        };

        properties.IndexingPolicy.CompositeIndexes.Add(compositeIndexes);

        ContainerResponse containerResponse = await resultDatabase.CreateContainerIfNotExistsAsync(properties, cancellationToken: cancellationToken);
        Container = containerResponse.Container;

        // check if the container has ttl enabled
        FeedIterator<ContainerProperties> resultSet = resultDatabase.GetContainerQueryIterator<ContainerProperties>($"select * from c where c.id = \"{Container.Id}\"");
        FeedResponse<ContainerProperties> queryProperties = await resultSet.ReadNextAsync(cancellationToken);
        ContainerProperties? containerSettings = queryProperties.Resource.FirstOrDefault();

        // check for ttl 
        if (containerSettings is { DefaultTimeToLive: null or > -1 })
        {
            throw new NotSupportedException($"{nameof(containerSettings.DefaultTimeToLive)} is not set to -1. Please set the value to -1");
        }

        // check for partition key 
        if (containerSettings is { PartitionKeyPath: not "/type" })
        {
            throw new NotSupportedException($"{nameof(containerSettings.PartitionKeyPath)} is not set to '/type'");
        }

        // create stored procedures 
        Assembly assembly = Assembly.GetExecutingAssembly();
        string[] storedProcedureFiles = assembly.GetManifestResourceNames().Where(n => n.EndsWith(".js")).ToArray();

        foreach (string storedProcedureFile in storedProcedureFiles)
        {
            logger.Info($"Creating storedprocedure : [{storedProcedureFile}]");
            using Stream? stream = assembly.GetManifestResourceStream(storedProcedureFile);

            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream), $"{storedProcedureFile} was not found");
            }

            using StreamReader streamReader = new (stream);

            StoredProcedureProperties sp = new ()
            {
                Body = await streamReader.ReadToEndAsync(),
                Id = Path.GetFileNameWithoutExtension(storedProcedureFile)
                    .Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries)
                    .Last()
            };

            const string query = "SELECT * FROM doc where doc.id = @Id";
            QueryDefinition queryDefinition = new (query);
            queryDefinition.WithParameter("@Id", sp.Id);

            using FeedIterator<StoredProcedureProperties> iterator = Container.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>(queryDefinition);

            if (iterator.HasMoreResults)
            {
                FeedResponse<StoredProcedureProperties> storedProcedure = await iterator.ReadNextAsync(cancellationToken);
                if (storedProcedure.Count == 0)
                {
                    await Container.Scripts.CreateStoredProcedureAsync(sp, cancellationToken: cancellationToken);
                }
                else
                {
                    await Container.Scripts.ReplaceStoredProcedureAsync(sp, cancellationToken: cancellationToken);
                }
            }
        }
    }
}