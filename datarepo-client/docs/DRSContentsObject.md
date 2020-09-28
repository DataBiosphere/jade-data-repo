
# DRSContentsObject

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **String** | A name declared by the bundle author that must be used when materialising this object, overriding any name directly associated with the object itself. The name must be unique with the containing bundle. This string is made up of uppercase and lowercase letters, decimal digits, hypen, period, and underscore [A-Za-z0-9.-_]. See http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_282[portable filenames]. | 
**id** | **String** | A DRS identifier of an &#x60;Object&#x60; (either a single blob or a nested bundle). If this ContentsObject is an object within a nested bundle, then the id is optional. Otherwise, the id is required. |  [optional]
**drsUri** | **List&lt;String&gt;** | A list of full DRS identifier URI paths that may be used to obtain the object. These URIs may be external to this DRS instance. |  [optional]
**contents** | [**List&lt;DRSContentsObject&gt;**](DRSContentsObject.md) | If this ContentsObject describes a nested bundle and the caller specified \&quot;?expand&#x3D;true\&quot; on the request, then this contents array must be present and describe the objects within the nested bundle. |  [optional]



