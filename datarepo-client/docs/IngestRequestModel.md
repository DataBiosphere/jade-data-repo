
# IngestRequestModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table** | **String** | name of the target table for ingest | 
**path** | **String** | gs path to a file in a bucket accessible to data repo | 
**format** | [**FormatEnum**](#FormatEnum) |  | 
**loadTag** | **String** |  |  [optional]
**maxBadRecords** | **Integer** | max number of bad records to skip; applies to JSON and CSV |  [optional]
**ignoreUnknownValues** | **Boolean** | skip extra data; applies to JSON and CSV |  [optional]
**csvFieldDelimiter** | **String** | field separator |  [optional]
**csvQuote** | **String** | quoting character |  [optional]
**csvSkipLeadingRows** | **Integer** | number of header rows to skip |  [optional]
**csvAllowQuotedNewlines** | **Boolean** |  |  [optional]
**csvNullMarker** | **String** |  |  [optional]


<a name="FormatEnum"></a>
## Enum: FormatEnum
Name | Value
---- | -----
CSV | &quot;csv&quot;
JSON | &quot;json&quot;



