
# ConfigFaultCountedModel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**skipFor** | **Integer** | number of fault tests to skip before beginning fault insertion |  [optional]
**insert** | **Integer** | total number of times to insert the fault; -1 means insert forever |  [optional]
**rate** | **Integer** | insert a fault rate percent of the time. If rate is 100, the fault will always be inserted regardless of rate.  |  [optional]
**rateStyle** | [**RateStyleEnum**](#RateStyleEnum) | fixed style means insert the fault; skip for rate-1; ... random style means randomly insert the fault with probability of 1:&lt;rate&gt;  |  [optional]


<a name="RateStyleEnum"></a>
## Enum: RateStyleEnum
Name | Value
---- | -----
FIXED | &quot;fixed&quot;
RANDOM | &quot;random&quot;



