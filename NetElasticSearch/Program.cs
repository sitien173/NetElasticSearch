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

app.MapGet("/mttq", ([FromQuery] string query, [FromServices] IElasticsearchService elasticsearchService, CancellationToken ct) => elasticsearchService.SearchDocumentsAsync(query, ct))
    .CacheOutput(opt =>
    {
        opt.Cache();
        opt.Expire(TimeSpan.FromHours(24));
    })
    .WithName("GetTransactionDocuments");

app.MapPost("/mttq", async (HttpRequest request, [FromServices] IElasticsearchService elasticsearchService, CancellationToken ct) =>
{
    var form = await request.ReadFormAsync(ct);
    var files = form.Files;
    
    if (files.Count == 0)
    {
        return Results.BadRequest("No files were uploaded.");
    }

    var tasks = files.Where(x => x is { Length: > 0, ContentType: "application/json" })
        .Select(file => Task.Run(async () =>
        {
            await using var jsonDataStream = file.OpenReadStream();
            await elasticsearchService.BulkIndexDocumentsAsync(JsonSerializer.DeserializeAsyncEnumerable<TransactionDocument>(jsonDataStream), ct).ConfigureAwait(false);
        }))
        .ToList();

    await Task.WhenAll(tasks).ConfigureAwait(false);

    return Results.Ok("Documents indexed successfully.");
    })
    .DisableRequestTimeout()
    .DisableAntiforgery()
    .WithName("IndexTransactionDocuments");

app.Run();
