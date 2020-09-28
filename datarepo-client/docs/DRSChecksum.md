
# DRSChecksum

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**checksum** | **String** | The hex-string encoded checksum for the data | 
**type** | **String** | The digest method used to create the checksum. If left unspecified md5 will be assumed.  possible values: md5                # most blob stores provide a checksum using this etag               # multipart uploads to blob stores sha256 sha512 |  [optional]



