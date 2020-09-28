# ResourcesApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createProfile**](ResourcesApi.md#createProfile) | **POST** /api/resources/v1/profiles | 
[**deleteProfile**](ResourcesApi.md#deleteProfile) | **DELETE** /api/resources/v1/profiles/{id} | 
[**enumerateProfiles**](ResourcesApi.md#enumerateProfiles) | **GET** /api/resources/v1/profiles | 
[**retrieveProfile**](ResourcesApi.md#retrieveProfile) | **GET** /api/resources/v1/profiles/{id} | 


<a name="createProfile"></a>
# **createProfile**
> BillingProfileModel createProfile(billingProfileRequest)



Creates a new profile associated with a billing account 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.ResourcesApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

ResourcesApi apiInstance = new ResourcesApi();
BillingProfileRequestModel billingProfileRequest = new BillingProfileRequestModel(); // BillingProfileRequestModel | 
try {
    BillingProfileModel result = apiInstance.createProfile(billingProfileRequest);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling ResourcesApi#createProfile");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **billingProfileRequest** | [**BillingProfileRequestModel**](BillingProfileRequestModel.md)|  | [optional]

### Return type

[**BillingProfileModel**](BillingProfileModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="deleteProfile"></a>
# **deleteProfile**
> DeleteResponseModel deleteProfile(id)



Delete a billing profile by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.ResourcesApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

ResourcesApi apiInstance = new ResourcesApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    DeleteResponseModel result = apiInstance.deleteProfile(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling ResourcesApi#deleteProfile");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**DeleteResponseModel**](DeleteResponseModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="enumerateProfiles"></a>
# **enumerateProfiles**
> EnumerateBillingProfileModel enumerateProfiles(offset, limit)



Returns a list of all of the billing profiles 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.ResourcesApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

ResourcesApi apiInstance = new ResourcesApi();
Integer offset = 0; // Integer | The number of items to skip before starting to collect the result set.
Integer limit = 10; // Integer | The numbers of items to return.
try {
    EnumerateBillingProfileModel result = apiInstance.enumerateProfiles(offset, limit);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling ResourcesApi#enumerateProfiles");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **offset** | **Integer**| The number of items to skip before starting to collect the result set. | [optional] [default to 0]
 **limit** | **Integer**| The numbers of items to return. | [optional] [default to 10]

### Return type

[**EnumerateBillingProfileModel**](EnumerateBillingProfileModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="retrieveProfile"></a>
# **retrieveProfile**
> BillingProfileModel retrieveProfile(id)



Retrieve a billing profile by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.ResourcesApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

ResourcesApi apiInstance = new ResourcesApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    BillingProfileModel result = apiInstance.retrieveProfile(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling ResourcesApi#retrieveProfile");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**BillingProfileModel**](BillingProfileModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

