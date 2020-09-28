
# DRSAccessMethod

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | [**TypeEnum**](#TypeEnum) | Type of the access method. | 
**accessUrl** | [**DRSAccessURL**](DRSAccessURL.md) | An &#x60;AccessURL&#x60; that can be used to fetch the actual object bytes. Note that at least one of &#x60;access_url&#x60; and &#x60;access_id&#x60; must be provided. |  [optional]
**accessId** | **String** | An arbitrary string to be passed to the &#x60;/access&#x60; path to get an &#x60;AccessURL&#x60;. This must be unique per object. Note that at least one of &#x60;access_url&#x60; and &#x60;access_id&#x60; must be provided. |  [optional]
**region** | **String** | Name of the region in the cloud service provider that the object belongs to. |  [optional]


<a name="TypeEnum"></a>
## Enum: TypeEnum
Name | Value
---- | -----
S3 | &quot;s3&quot;
GS | &quot;gs&quot;
FTP | &quot;ftp&quot;
GSIFTP | &quot;gsiftp&quot;
GLOBUS | &quot;globus&quot;
HTSGET | &quot;htsget&quot;
HTTPS | &quot;https&quot;
FILE | &quot;file&quot;



