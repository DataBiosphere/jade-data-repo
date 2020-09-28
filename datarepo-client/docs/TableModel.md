
# TableModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **String** |  | 
**columns** | [**List&lt;ColumnModel&gt;**](ColumnModel.md) |  | 
**primaryKey** | **List&lt;String&gt;** |  |  [optional]
**partitionMode** | [**PartitionModeEnum**](#PartitionModeEnum) |  |  [optional]
**datePartitionOptions** | [**DatePartitionOptionsModel**](DatePartitionOptionsModel.md) |  |  [optional]
**intPartitionOptions** | [**IntPartitionOptionsModel**](IntPartitionOptionsModel.md) |  |  [optional]
**rowCount** | **Integer** |  |  [optional]


<a name="PartitionModeEnum"></a>
## Enum: PartitionModeEnum
Name | Value
---- | -----
NONE | &quot;none&quot;
DATE | &quot;date&quot;
INT | &quot;int&quot;



