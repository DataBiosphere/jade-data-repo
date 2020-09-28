
# IntPartitionOptionsModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**column** | **String** | The INT64 or INTEGER column in the table to partition on.  | 
**min** | **Long** | The smallest value to partition within the target column. Any rows with a value smaller than this will be unpartitioned.  | 
**max** | **Long** | The largest value to partition within the target column. Any rows with a value larger than this will be unpartitioned.  | 
**interval** | **Long** | The size to use when dividing the partitioning range into \&quot;buckets\&quot;. (max - min) / (this value) cannot be larger than 4,000.  | 



