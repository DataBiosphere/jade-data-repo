
# ConfigModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **String** | name of the configuration |  [optional]
**configType** | [**ConfigTypeEnum**](#ConfigTypeEnum) |  |  [optional]
**fault** | [**ConfigFaultModel**](ConfigFaultModel.md) |  |  [optional]
**parameter** | [**ConfigParameterModel**](ConfigParameterModel.md) |  |  [optional]


<a name="ConfigTypeEnum"></a>
## Enum: ConfigTypeEnum
Name | Value
---- | -----
FAULT | &quot;fault&quot;
PARAMETER | &quot;parameter&quot;
LOGGING | &quot;logging&quot;



