
# SnapshotRequestContentsModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**datasetName** | **String** |  | 
**mode** | [**ModeEnum**](#ModeEnum) |  | 
**assetSpec** | [**SnapshotRequestAssetModel**](SnapshotRequestAssetModel.md) |  |  [optional]
**querySpec** | [**SnapshotRequestQueryModel**](SnapshotRequestQueryModel.md) |  |  [optional]
**rowIdSpec** | [**SnapshotRequestRowIdModel**](SnapshotRequestRowIdModel.md) |  |  [optional]


<a name="ModeEnum"></a>
## Enum: ModeEnum
Name | Value
---- | -----
BYASSET | &quot;byAsset&quot;
BYFULLVIEW | &quot;byFullView&quot;
BYQUERY | &quot;byQuery&quot;
BYROWID | &quot;byRowId&quot;



