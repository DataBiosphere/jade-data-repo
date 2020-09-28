
# BulkLoadFileResultModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**sourcePath** | **String** | gs URL of the source file to load | 
**targetPath** | **String** | Full path within the dataset where the file should be placed. The path must start with /.  | 
**state** | [**BulkLoadFileState**](BulkLoadFileState.md) |  |  [optional]
**fileId** | **String** | The fileId of the loaded file; non-null if state is SUCCEEDED |  [optional]
**error** | **String** | The error message if state is FAILED |  [optional]



