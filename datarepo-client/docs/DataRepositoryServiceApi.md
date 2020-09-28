# DataRepositoryServiceApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getAccessURL**](DataRepositoryServiceApi.md#getAccessURL) | **GET** /ga4gh/drs/v1/objects/{object_id}/access/{access_id} | Get a URL for fetching bytes.
[**getObject**](DataRepositoryServiceApi.md#getObject) | **GET** /ga4gh/drs/v1/objects/{object_id} | Get info about an &#x60;Object&#x60;.
[**getServiceInfo**](DataRepositoryServiceApi.md#getServiceInfo) | **GET** /ga4gh/drs/v1/service-info | Get information about this implementation.


<a name="getAccessURL"></a>
# **getAccessURL**
> DRSAccessURL getAccessURL(objectId, accessId)

Get a URL for fetching bytes.

Returns a URL that can be used to fetch the object bytes. This method only needs to be called when using an &#x60;AccessMethod&#x60; that contains an &#x60;access_id&#x60; (e.g., for servers that use signed URLs for fetching object bytes).

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.DataRepositoryServiceApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

DataRepositoryServiceApi apiInstance = new DataRepositoryServiceApi();
String objectId = "objectId_example"; // String | An `id` of a Data Object
String accessId = "accessId_example"; // String | An `access_id` from the `access_methods` list of a Data Object
try {
    DRSAccessURL result = apiInstance.getAccessURL(objectId, accessId);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling DataRepositoryServiceApi#getAccessURL");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **objectId** | **String**| An &#x60;id&#x60; of a Data Object |
 **accessId** | **String**| An &#x60;access_id&#x60; from the &#x60;access_methods&#x60; list of a Data Object |

### Return type

[**DRSAccessURL**](DRSAccessURL.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="getObject"></a>
# **getObject**
> DRSObject getObject(objectId, expand)

Get info about an &#x60;Object&#x60;.

Returns object metadata, and a list of access methods that can be used to fetch object bytes.

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.DataRepositoryServiceApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

DataRepositoryServiceApi apiInstance = new DataRepositoryServiceApi();
String objectId = "objectId_example"; // String | 
Boolean expand = false; // Boolean | If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains another bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored.
try {
    DRSObject result = apiInstance.getObject(objectId, expand);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling DataRepositoryServiceApi#getObject");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **objectId** | **String**|  |
 **expand** | **Boolean**| If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains another bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. | [optional] [default to false]

### Return type

[**DRSObject**](DRSObject.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="getServiceInfo"></a>
# **getServiceInfo**
> DRSServiceInfo getServiceInfo()

Get information about this implementation.

May return service version and other information. [dd]NOTE: technically, this has been removed from DRS V1.0. It will be added back when there is a common service_info across ga4gh. I don&#39;t expect it to be too different, so just leaving this info call in place.

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.DataRepositoryServiceApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

DataRepositoryServiceApi apiInstance = new DataRepositoryServiceApi();
try {
    DRSServiceInfo result = apiInstance.getServiceInfo();
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling DataRepositoryServiceApi#getServiceInfo");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**DRSServiceInfo**](DRSServiceInfo.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

