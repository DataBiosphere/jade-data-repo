
# JobModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** |  | 
**description** | **String** | Description of the job&#39;s flight from description flight input parameter |  [optional]
**jobStatus** | [**JobStatusEnum**](#JobStatusEnum) | Status of job | 
**statusCode** | **Integer** | HTTP code | 
**submitted** | **String** | Timestamp when the flight was created |  [optional]
**completed** | **String** | Timestamp when the flight was completed; not present if not complete |  [optional]


<a name="JobStatusEnum"></a>
## Enum: JobStatusEnum
Name | Value
---- | -----
RUNNING | &quot;running&quot;
SUCCEEDED | &quot;succeeded&quot;
FAILED | &quot;failed&quot;



