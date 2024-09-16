using System.Text.Json.Serialization;

namespace NetElasticSearch;

public class TransactionDocument
{
    [JsonPropertyName("date")]
    public string Date { get; set; }

    [JsonPropertyName("trans_id")]
    public string TransactionID { get; set; }

    [JsonPropertyName("amount")]
    public string Amount { get; set; }

    [JsonPropertyName("message")]
    public string Message { get; set; }
}

