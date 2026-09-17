using Dapper;
using Npgsql;
using Telemetry.Api;
using Telemetry.Core;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Timescale")
    ?? throw new InvalidOperationException("ConnectionStrings:Timescale is not configured.");

builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.Configure<MqttSettings>(builder.Configuration.GetSection("Mqtt"));
builder.Services.AddSingleton<LiveTelemetryCache>();
builder.Services.AddHostedService<LiveTelemetryService>();
builder.Services.AddSignalR();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapHub<TelemetryHub>("/hubs/telemetry");

app.MapGet("/health", async (NpgsqlDataSource db) =>
{
    await using var connection = await db.OpenConnectionAsync();
    await connection.ExecuteScalarAsync<int>("SELECT 1");
    return Results.Ok(new { status = "healthy" });
});

var api = app.MapGroup("/api");

// Latest reading per device, using DISTINCT ON over the (device_id, time DESC) index.
api.MapGet("/devices", async (NpgsqlDataSource db) =>
{
    await using var connection = await db.OpenConnectionAsync();
    return await connection.QueryAsync<DeviceReading>(Queries.LatestPerDevice);
});

api.MapGet("/devices/{deviceId}/readings", async (string deviceId, int? minutes, NpgsqlDataSource db) =>
{
    var window = Math.Clamp(minutes ?? 15, 1, 1440);
    await using var connection = await db.OpenConnectionAsync();
    return await connection.QueryAsync<DeviceReading>(Queries.RawReadings, new { deviceId, minutes = window });
});

// 1-minute rollups served from the continuous aggregate instead of scanning raw rows.
api.MapGet("/devices/{deviceId}/aggregates", async (string deviceId, int? hours, NpgsqlDataSource db) =>
{
    var window = Math.Clamp(hours ?? 1, 1, 168);
    await using var connection = await db.OpenConnectionAsync();
    return await connection.QueryAsync<MinuteAggregate>(Queries.MinuteAggregates, new { deviceId, hours = window });
});

api.MapGet("/stats", async (NpgsqlDataSource db) =>
{
    await using var connection = await db.OpenConnectionAsync();
    return await connection.QuerySingleAsync<StorageStats>(Queries.Stats);
});

app.Run();
