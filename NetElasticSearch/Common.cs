using System.Text.RegularExpressions;

namespace NetElasticSearch;

public static class TransactionRegex
{
    public static readonly Regex DATE_REGEX = new(@"^\d{2}/\d{2}/\d{4}$", RegexOptions.Compiled);
    public static readonly Regex AMOUNT_REGEX = new(@"^(\d{1,3}\.*\d{3}\.*\d{3}\.*\d{3}\.000|\d{1,3}\.*\d{3}\.*\d{3}\.000|\d{1,3}\.*\d{3}\.000|\d{1,3}\.000)$", RegexOptions.Compiled);
    public static readonly Regex TRXNID_REGEX = new(@"^\d{4,5}\.\d{2,9}$", RegexOptions.Compiled);
}