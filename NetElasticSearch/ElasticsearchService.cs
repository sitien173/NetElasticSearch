using System.Linq.Expressions;
using System.Text.RegularExpressions;
using Nest;

namespace NetElasticSearch;

public interface IElasticsearchService
{
    Task BulkIndexDocumentsAsync(IAsyncEnumerable<TransactionDocument?> documents, CancellationToken ct);
    Task<IEnumerable<TransactionDocument>> SearchDocumentsAsync(string query, CancellationToken ct);
}

public class ElasticsearchService(IElasticClient client, ILogger<IElasticsearchService> logger) : IElasticsearchService
{
    public async Task BulkIndexDocumentsAsync(IAsyncEnumerable<TransactionDocument?> documents, CancellationToken ct)
    {
        logger.LogInformation("Starting bulk indexing documents");
        var bulkRequest = new BulkDescriptor();
        var count = 0;
        var indexedDocuments = new List<TransactionDocument>();
        var batchSize = 1000;

        await foreach (var document in documents.WithCancellation(ct))
        {
            if (document == null)
            {
                continue;
            }
            
            indexedDocuments.Add(document);
            bulkRequest.Index<TransactionDocument>(op => op.Document(document));

            count++;

            // When we reach the batch size, send the bulk request
            if (count % batchSize != 0) 
                continue;
            
            count = 0;
            await SendBulkRequestAsync(bulkRequest, ct).ConfigureAwait(false);
            bulkRequest = new BulkDescriptor(); // Reset the bulk request
            indexedDocuments.Clear(); // Clear the local list
        }

        // If there are any remaining documents after the loop, send one last bulk request
        if (indexedDocuments.Count > 0)
        {
            await SendBulkRequestAsync(bulkRequest, ct).ConfigureAwait(false);
        }
        logger.LogInformation("Finished bulk indexing documents");
    }

    private async Task SendBulkRequestAsync(BulkDescriptor bulkRequest, CancellationToken ct)
    {
        logger.LogInformation("Sending bulk request with {Count} documents", ((IBulkRequest)bulkRequest).Operations.Count);
        var bulkResponse = await client.BulkAsync(bulkRequest, ct).ConfigureAwait(false);
        
        if (bulkResponse.Errors)
        {
            logger.LogError("Bulk request failed with {Error}", bulkResponse.DebugInformation);
        }
    }

    public Task<IEnumerable<TransactionDocument>> SearchDocumentsAsync(string query, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return Task.FromResult(Enumerable.Empty<TransactionDocument>());
        }

        logger.LogInformation("Searching documents with query {Query}", query);

        var regexFieldMap = new Dictionary<Regex, Expression<Func<TransactionDocument, object>>>
        {
            { TransactionRegex.AMOUNT_REGEX, f => f.Amount.Suffix("keyword") },
            { TransactionRegex.DATE_REGEX, f => f.Date.Suffix("keyword") },
            { TransactionRegex.TRXNID_REGEX, f => f.TransactionID.Suffix("keyword") }
        };

        foreach (var (regex, field) in regexFieldMap)
        {
            if (regex.IsMatch(query))
            {
                return ExecuteScrollQueryAsync(q => q
                    .Term(t => t
                        .Field(field)
                        .Value(query)
                    ), ct);
            }
        }

        return ExecuteScrollQueryAsync(q => q
            .MatchPhrase(m => m
                    .Field(f => f.Message)
                    .Query(query)
            ), ct);
    }

    private async Task<IEnumerable<TransactionDocument>> ExecuteScrollQueryAsync(
        Func<QueryContainerDescriptor<TransactionDocument>, QueryContainer> query, CancellationToken ct)
    {
        var scrollTimeout = "2m"; // Scroll timeout duration
        var scrollSize = 1000; // Number of documents per scroll batch

        var searchResponse = await client.SearchAsync<TransactionDocument>(s => s
            .Scroll(scrollTimeout)
            .Size(scrollSize)
            .Query(query), ct).ConfigureAwait(false);

        var results = new List<TransactionDocument>(searchResponse.Documents);

        // Continue scrolling until no more documents
        while (searchResponse.Documents.Any())
        {
            var scrollId = searchResponse.ScrollId;

            searchResponse = await client.ScrollAsync<TransactionDocument>(scrollTimeout, scrollId, ct: ct)
                .ConfigureAwait(false);
            results.AddRange(searchResponse.Documents);
        }

        // Clear the scroll context when done
        await client.ClearScrollAsync(new ClearScrollRequest(searchResponse.ScrollId), ct);

        return results;
    }
}