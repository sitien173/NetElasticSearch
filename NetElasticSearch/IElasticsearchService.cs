using Nest;

namespace NetElasticSearch;

public interface IElasticsearchService
{
    Task BulkIndexDocumentsAsync<T>(IAsyncEnumerable<T> documents, CancellationToken ct) where T : class;
    Task<IEnumerable<T>> SearchDocumentsAsync<T>(string query, CancellationToken ct) where T : class;
}

public class ElasticsearchService(IElasticClient client, ILogger<IElasticsearchService> logger) : IElasticsearchService
{
    public async Task BulkIndexDocumentsAsync<T>(IAsyncEnumerable<T> documents, CancellationToken ct) where T : class
    {
        logger.LogInformation("Starting bulk indexing documents");
        var bulkRequest = new BulkDescriptor();
        var count = 0;
        var indexedDocuments = new List<T>();
        var batchSize = 1000;

        await foreach (var document in documents.WithCancellation(ct))
        {
            indexedDocuments.Add(document);
            bulkRequest.Index<T>(op => op.Document(document));

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
        
        if (!bulkResponse.IsValid)
        {
            logger.LogError("Bulk request failed with {Error}", bulkResponse.DebugInformation);
        }
    }

    public async Task IndexDocumentAsync<T>(T document, CancellationToken ct) where T : class
    {
        logger.LogInformation("Indexing document {@Document}", document);
        var response = await client.IndexDocumentAsync(document, ct).ConfigureAwait(false);
        
        if (!response.IsValid)
        {
            logger.LogError("Indexing document failed with {Error}", response.DebugInformation);
        }
    }

    public async Task<T> GetDocumentAsync<T>(string id, CancellationToken ct) where T : class
    {
        logger.LogInformation("Getting document with ID {Id}", id);
        var response = await client.GetAsync<T>(id, ct: ct).ConfigureAwait(false);
        
        if (!response.IsValid)
        {
            logger.LogError("Getting document failed with {Error}", response.DebugInformation);
        }
        
        return response.Source;
    }

    public async Task<IEnumerable<T>> SearchDocumentsAsync<T>(string query, CancellationToken ct) where T : class
    {
        logger.LogInformation("Searching documents with query {Query}", query);
        var searchResponse = await client.SearchAsync<T>(s => s
            .Query(q => q
                .QueryString(d => d
                    .Query(query)
                )   
            ).Size(100), ct).ConfigureAwait(false);
        
        if (!searchResponse.IsValid)
        {
            logger.LogError("Searching documents failed with {Error}", searchResponse.DebugInformation);
        }
        
        return searchResponse.Documents;
    }
}