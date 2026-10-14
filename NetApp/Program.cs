using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using NetApp.Helpers;

namespace NetApp;

class Program
{
    static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);


        // Register appsettings.json configuration file
        builder.Configuration.AddJsonFile("appsettings.json", optional: false);


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
        builder.Services.AddHostedService<KafkaConsumerService>();


        var app = builder.Build();


        Console.WriteLine("** App is running. **");
        app.Run();

    }
}
