
# DatasetModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** |  |  [optional]
**name** | **String** |  |  [optional]
**description** | **String** | Description of the dataset |  [optional]
**defaultProfileId** | **String** | This is the profile id used for core dataset resources |  [optional]
**dataProject** | **String** | Project id of the project where tabular data in BigQuery lives |  [optional]
**additionalProfileIds** | **List&lt;String&gt;** | Additional profile ids to be used for this dataset |  [optional]
**defaultSnapshotId** | **String** | Id of the auto-generated default passthru snapshot |  [optional]
**schema** | [**DatasetSpecificationModel**](DatasetSpecificationModel.md) |  |  [optional]
**createdDate** | **String** | Date the dataset was created |  [optional]



