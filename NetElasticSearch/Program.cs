using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Nest;
using NetElasticSearch;
using Serilog;

var builder = WebApplication.CreateSlimBuilder(args);

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();

// Replace the default logger
builder.Host.UseSerilog();

builder.WebHost.UseKestrel(opt =>
{
    opt.Limits.MaxRequestBodySize = long.MaxValue;
});

builder.Services.AddOutputCache();

var settings = new ConnectionSettings(new Uri(builder.Configuration["Elasticsearch:Uri"]!))
    .DefaultIndex(builder.Configuration["Elasticsearch:DefaultIndex"]!);

var client = new ElasticClient(settings);
builder.Services.AddSingleton<IElasticClient>(client);
builder.Services.AddScoped<IElasticsearchService, ElasticsearchService>();

var app = builder.Build();

app.UseHttpsRedirection();
app.UseOutputCache();
app.UseSerilogRequestLogging();

app.MapGet("/mttq", ([FromQuery] string query, [FromServices] IElasticsearchService elasticsearchService, CancellationToken ct) => elasticsearchService.SearchDocumentsAsync<TransactionDocument>(query, ct))
    .CacheOutput(opt =>
    {
        opt.Cache();
        opt.Expire(TimeSpan.FromHours(24));
    })
    .WithName("GetTransactionDocuments");

app.MapPost("/mttq", async ([FromForm(Name = "json-file")] IFormFile file, [FromServices] IElasticsearchService elasticsearchService, CancellationToken ct) =>
    {
        await using var jsonDataStream = file.OpenReadStream();
        await elasticsearchService.BulkIndexDocumentsAsync(JsonSerializer.DeserializeAsyncEnumerable<TransactionDocument>(jsonDataStream), ct).ConfigureAwait(false);
        
        return Results.Ok("Documents indexed successfully.");
    })
    .DisableRequestTimeout()
    .DisableAntiforgery()
    .WithName("IndexTransactionDocuments");

app.Run();
