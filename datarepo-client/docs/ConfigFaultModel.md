
# ConfigFaultModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**enabled** | **Boolean** | If the fault is enabled, then is in effect. Fault points cause insert faults. Typical usage is that faults are disabled on system start and explicitly enabled by test code or via the setFault endpoint.  |  [optional]
**faultType** | [**FaultTypeEnum**](#FaultTypeEnum) | A simple fault has no parameters. It is just enabled or disabled. This type of fault is typically used when the desired behavior of the fault is too complex for expression in the fault types and custom code is needed to get the right failure behavior.  A counted fault is used to insert some number of faults in a pattern. See the ConfigFaultCountedModel for details.  |  [optional]
**counted** | [**ConfigFaultCountedModel**](ConfigFaultCountedModel.md) |  |  [optional]


<a name="FaultTypeEnum"></a>
## Enum: FaultTypeEnum
Name | Value
---- | -----
SIMPLE | &quot;simple&quot;
COUNTED | &quot;counted&quot;



