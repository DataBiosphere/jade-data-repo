
# FileModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**fileId** | **String** | Unique id of the filesystem object within the dataset |  [optional]
**collectionId** | **String** | Id of the dataset or snapshot directory describing the object |  [optional]
**path** | **String** | full path of the file in the dataset |  [optional]
**size** | **Long** | Always present for files - the file size in bytes Present for directories in snapshots - sum of sizes of objects in a directory  |  [optional]
**checksums** | [**List&lt;DRSChecksum&gt;**](DRSChecksum.md) | Always present for files - checksums; always includes crc32c. May include md5. Present for directories in snapshots - see DRS spec for algorithm for combining checksums of underlying directory contents.  |  [optional]
**created** | **String** | timestamp of object creation in RFC3339 |  [optional]
**description** | **String** | Human readable description of the file |  [optional]
**fileType** | [**FileModelType**](FileModelType.md) |  |  [optional]
**fileDetail** | [**FileDetailModel**](FileDetailModel.md) |  |  [optional]
**directoryDetail** | [**DirectoryDetailModel**](DirectoryDetailModel.md) |  |  [optional]



