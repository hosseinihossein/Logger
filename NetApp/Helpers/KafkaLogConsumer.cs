using Confluent.Kafka;
using MongoDB.Bson;
using MongoDB.Driver;

namespace NetApp.Helpers;

public class KafkaLogConsumer : BackgroundService
{
    private readonly ILogger<KafkaLogConsumer> _logger;
    private readonly ConsumerConfig _config;
    private readonly string _topicName;
    private readonly IMongoCollection<BsonDocument> _sysWarnsCollection;
    private readonly IMongoCollection<BsonDocument> _adminMsgsCollection;
    //private readonly IMongoDatabase _mongoDb;

    public KafkaLogConsumer(ILogger<KafkaLogConsumer> logger, IConfiguration configuration,
    IMongoDatabase mongoDb)
    {
        _logger = logger;

        // Define Kafka consumer configurations
        _config = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServer"] ?? "kafka:9092",
            GroupId = configuration["Kafka:GroupId"] ?? "logger-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true // Or false if you intend to commit manually
        };

        _topicName = configuration["Kafka:LogTopic"] ?? "log";

        _sysWarnsCollection = mongoDb.GetCollection<BsonDocument>(configuration["MongoDB:SystemWarningsCollectionName"] ?? "Logs");
        _adminMsgsCollection = mongoDb.GetCollection<BsonDocument>(configuration["MongoDB:AdminMessagesCollectionName"] ?? "Logs");
        //_mongoDb = mongoDb;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Execute the consumer loop on a background thread to prevent blocking application startup
        return Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);
    }

    private void ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        // Build the Kafka consumer instance
        using var consumer = new ConsumerBuilder<Ignore, string>(_config)
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError($"Kafka Consumer Error: {e.Reason}");
            })
            .Build();

        consumer.Subscribe(_topicName);
        _logger.LogInformation($"Subscribed to topic: {_topicName}");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Polls for new messages. Pass stoppingToken to handle graceful shutdown.
                    var consumeResult = consumer.Consume(stoppingToken);

                    if (consumeResult != null)
                    {
                        //_logger.LogInformation($"Received message: '{consumeResult.Message.Value}' from partition: {consumeResult.Partition}");

                        // TODO: Implement your business logic / message handling here
                        string logJson = consumeResult.Message.Value;
                        if (BsonDocument.TryParse(logJson, out BsonDocument logDocument))
                        {
                            IMongoCollection<BsonDocument> mongoCollection = _sysWarnsCollection;
                            if (logDocument.TryGetValue("Message", out BsonValue messageBsonValue) && messageBsonValue.IsString)
                            {
                                if (BsonDocument.TryParse(messageBsonValue.AsString, out BsonDocument messageDoc))
                                {
                                    logDocument["Message"] = messageDoc;
                                    mongoCollection = _adminMsgsCollection;
                                }
                            }
                            mongoCollection.InsertOne(logDocument);
                            //Console.WriteLine($"consumed: {logDocument.ToJson()}");
                        }
                        else
                        {
                            _logger.LogWarning($"Can not parse consumed logJson to logDocument of type BsonDocument! logJson: {logJson}");
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    // Errors occurring during consumption (e.g., deserialization issues)
                    _logger.LogError($"Consume Error occurred during consumption: {e.Error.Reason}");
                }
                catch (Exception e)
                {
                    // Errors occurring during consumption (e.g., deserialization issues)
                    _logger.LogError($"Error occurred during consumption: {e}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Kafka consumption loop canceled via termination token.");
        }
        finally
        {
            // Close down cleanly, committing remaining offsets and leaving the consumer group gracefully
            consumer.Close();
            _logger.LogWarning("Kafka consumer connection closed cleanly.");
            consumer.Dispose();
        }
    }
}