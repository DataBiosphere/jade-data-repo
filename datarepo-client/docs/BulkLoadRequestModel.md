
# BulkLoadRequestModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**profileId** | **String** | The profile id to use for these files | 
**loadTag** | **String** |  | 
**maxFailedFileLoads** | **Integer** | max number of failed file loads before stopping |  [optional]
**loadControlFile** | **String** | gs:// path to a text file in a bucket. The file must be accessible to the DR Manager. Each line of the file is interpreted as the JSON form of one BulkLoadFileModel. For example, one line might look like   &#39;{ \&quot;sourcePath\&quot;:\&quot;gs:/bucket/path/file\&quot;, \&quot;targetPath\&quot;:\&quot;/target/path/file\&quot; }&#39; | 



