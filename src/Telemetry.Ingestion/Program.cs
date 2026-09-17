using System.Threading.Channels;
using Microsoft.Extensions.Options;
using Npgsql;
using Telemetry.Core;
using Telemetry.Ingestion;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<MqttSettings>(builder.Configuration.GetSection("Mqtt"));
builder.Services.Configure<IngestionSettings>(builder.Configuration.GetSection("Ingestion"));
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));

var ingestion = builder.Configuration.GetSection("Ingestion").Get<IngestionSettings>() ?? new IngestionSettings();
var connectionString = builder.Configuration.GetConnectionString("Timescale")
    ?? throw new InvalidOperationException("ConnectionStrings:Timescale is not configured.");

builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<IngestionMetrics>();

var channel = Channel.CreateBounded<TelemetryReading>(new BoundedChannelOptions(ingestion.QueueCapacity)
{
    FullMode = BoundedChannelFullMode.Wait,
    SingleWriter = true,
    SingleReader = ingestion.WriterCount == 1,
});
builder.Services.AddSingleton(channel.Reader);
builder.Services.AddSingleton(channel.Writer);

// Hosted services stop in reverse registration order. Writers are registered first so the
// MQTT listener stops first, completes the channel, and the writers then drain the buffer.
for (var i = 0; i < ingestion.WriterCount; i++)
{
    var writerId = i;
    builder.Services.AddSingleton<IHostedService>(sp => new TimescaleBatchWriter(
        writerId,
        sp.GetRequiredService<ChannelReader<TelemetryReading>>(),
        sp.GetRequiredService<NpgsqlDataSource>(),
        sp.GetRequiredService<IngestionMetrics>(),
        sp.GetRequiredService<IOptions<IngestionSettings>>(),
        sp.GetRequiredService<ILogger<TimescaleBatchWriter>>()));
}

builder.Services.AddHostedService<MetricsReporter>();
builder.Services.AddHostedService<MqttIngestionService>();

builder.Build().Run();
