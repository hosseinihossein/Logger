using MongoDB.Driver;
using NetApp.Helpers;

namespace NetApp;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);


        // Register IMongoClient as a Singleton
        IMongoClient mongoClient = new MongoClient(builder.Configuration["MongoDb:ConnectionString"]);
        builder.Services.AddSingleton<IMongoClient>(mongoClient);
        // Register IMongoDatabase directly as a Singleton for quick access
        IMongoDatabase mongoDatabase = mongoClient.GetDatabase(builder.Configuration["MongoDb:DatabaseName"]);
        builder.Services.AddSingleton<IMongoDatabase>(mongoDatabase);


        /*/ Setup built-in logging engine configuration
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole(); // Keep console for local visual debugging
        builder.Logging.AddMongoDb(mongoDatabase, builder.Configuration["MongoDB:CollectionName"] ?? "Logs");
        */


        // Register the Kafka Consumer Service as a Hosted Service
        builder.Services.AddHostedService<KafkaLogConsumer>();


        var host = builder.Build();


        Console.WriteLine("** App is running. **");
        host.Run();
    }
}
