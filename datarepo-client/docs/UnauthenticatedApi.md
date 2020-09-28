# UnauthenticatedApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**retrieveRepositoryConfig**](UnauthenticatedApi.md#retrieveRepositoryConfig) | **GET** /configuration | 
[**serviceStatus**](UnauthenticatedApi.md#serviceStatus) | **GET** /status | 
[**shutdownRequest**](UnauthenticatedApi.md#shutdownRequest) | **GET** /shutdown | 


<a name="retrieveRepositoryConfig"></a>
# **retrieveRepositoryConfig**
> RepositoryConfigurationModel retrieveRepositoryConfig()



Retrieve the repository configuration information

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.api.UnauthenticatedApi;


UnauthenticatedApi apiInstance = new UnauthenticatedApi();
try {
    RepositoryConfigurationModel result = apiInstance.retrieveRepositoryConfig();
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling UnauthenticatedApi#retrieveRepositoryConfig");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**RepositoryConfigurationModel**](RepositoryConfigurationModel.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="serviceStatus"></a>
# **serviceStatus**
> serviceStatus()



Returns the operational status of the service 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.api.UnauthenticatedApi;


UnauthenticatedApi apiInstance = new UnauthenticatedApi();
try {
    apiInstance.serviceStatus();
} catch (ApiException e) {
    System.err.println("Exception when calling UnauthenticatedApi#serviceStatus");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="shutdownRequest"></a>
# **shutdownRequest**
> shutdownRequest()



Requests that this instance of DR Manager shut down. In production, this must be configured to only be callable by Kubernetes. 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.api.UnauthenticatedApi;


UnauthenticatedApi apiInstance = new UnauthenticatedApi();
try {
    apiInstance.shutdownRequest();
} catch (ApiException e) {
    System.err.println("Exception when calling UnauthenticatedApi#shutdownRequest");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

