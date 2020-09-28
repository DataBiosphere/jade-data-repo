# RepositoryApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addDatasetAssetSpecifications**](RepositoryApi.md#addDatasetAssetSpecifications) | **POST** /api/repository/v1/datasets/{id}/assets | 
[**addDatasetPolicyMember**](RepositoryApi.md#addDatasetPolicyMember) | **POST** /api/repository/v1/datasets/{id}/policies/{policyName}/members | 
[**addSnapshotPolicyMember**](RepositoryApi.md#addSnapshotPolicyMember) | **POST** /api/repository/v1/snapshots/{id}/policies/{policyName}/members | 
[**applyDatasetDataDeletion**](RepositoryApi.md#applyDatasetDataDeletion) | **POST** /api/repository/v1/datasets/{id}/deletes | 
[**bulkFileLoad**](RepositoryApi.md#bulkFileLoad) | **POST** /api/repository/v1/datasets/{id}/files/bulk | 
[**bulkFileLoadArray**](RepositoryApi.md#bulkFileLoadArray) | **POST** /api/repository/v1/datasets/{id}/files/bulk/array | 
[**bulkFileResultsDelete**](RepositoryApi.md#bulkFileResultsDelete) | **DELETE** /api/repository/v1/datasets/{id}/files/bulk/{loadtag} | 
[**bulkFileResultsGet**](RepositoryApi.md#bulkFileResultsGet) | **GET** /api/repository/v1/datasets/{id}/files/bulk/{loadtag} | 
[**createDataset**](RepositoryApi.md#createDataset) | **POST** /api/repository/v1/datasets | 
[**createSnapshot**](RepositoryApi.md#createSnapshot) | **POST** /api/repository/v1/snapshots | 
[**deleteDataset**](RepositoryApi.md#deleteDataset) | **DELETE** /api/repository/v1/datasets/{id} | 
[**deleteDatasetPolicyMember**](RepositoryApi.md#deleteDatasetPolicyMember) | **DELETE** /api/repository/v1/datasets/{id}/policies/{policyName}/members/{memberEmail} | 
[**deleteFile**](RepositoryApi.md#deleteFile) | **DELETE** /api/repository/v1/datasets/{id}/files/{fileid} | 
[**deleteJob**](RepositoryApi.md#deleteJob) | **DELETE** /api/repository/v1/jobs/{id} | 
[**deleteSnapshot**](RepositoryApi.md#deleteSnapshot) | **DELETE** /api/repository/v1/snapshots/{id} | 
[**deleteSnapshotPolicyMember**](RepositoryApi.md#deleteSnapshotPolicyMember) | **DELETE** /api/repository/v1/snapshots/{id}/policies/{policyName}/members/{memberEmail} | 
[**enumerateDatasets**](RepositoryApi.md#enumerateDatasets) | **GET** /api/repository/v1/datasets | 
[**enumerateJobs**](RepositoryApi.md#enumerateJobs) | **GET** /api/repository/v1/jobs | 
[**enumerateSnapshots**](RepositoryApi.md#enumerateSnapshots) | **GET** /api/repository/v1/snapshots | 
[**getConfig**](RepositoryApi.md#getConfig) | **GET** /api/repository/v1/configs/{name} | 
[**getConfigList**](RepositoryApi.md#getConfigList) | **GET** /api/repository/v1/configs | 
[**ingestDataset**](RepositoryApi.md#ingestDataset) | **POST** /api/repository/v1/datasets/{id}/ingest | 
[**ingestFile**](RepositoryApi.md#ingestFile) | **POST** /api/repository/v1/datasets/{id}/files | 
[**lookupFileById**](RepositoryApi.md#lookupFileById) | **GET** /api/repository/v1/datasets/{id}/files/{fileid} | 
[**lookupFileByPath**](RepositoryApi.md#lookupFileByPath) | **GET** /api/repository/v1/datasets/{id}/filesystem/objects | 
[**lookupSnapshotFileById**](RepositoryApi.md#lookupSnapshotFileById) | **GET** /api/repository/v1/snapshots/{id}/files/{fileid} | 
[**lookupSnapshotFileByPath**](RepositoryApi.md#lookupSnapshotFileByPath) | **GET** /api/repository/v1/snapshots/{id}/filesystem/objects | 
[**removeDatasetAssetSpecifications**](RepositoryApi.md#removeDatasetAssetSpecifications) | **DELETE** /api/repository/v1/datasets/{id}/assets/{assetid} | 
[**resetConfig**](RepositoryApi.md#resetConfig) | **PUT** /api/repository/v1/configs/reset | 
[**retrieveDataset**](RepositoryApi.md#retrieveDataset) | **GET** /api/repository/v1/datasets/{id} | 
[**retrieveDatasetPolicies**](RepositoryApi.md#retrieveDatasetPolicies) | **GET** /api/repository/v1/datasets/{id}/policies | 
[**retrieveJob**](RepositoryApi.md#retrieveJob) | **GET** /api/repository/v1/jobs/{id} | 
[**retrieveJobResult**](RepositoryApi.md#retrieveJobResult) | **GET** /api/repository/v1/jobs/{id}/result | 
[**retrieveSnapshot**](RepositoryApi.md#retrieveSnapshot) | **GET** /api/repository/v1/snapshots/{id} | 
[**retrieveSnapshotPolicies**](RepositoryApi.md#retrieveSnapshotPolicies) | **GET** /api/repository/v1/snapshots/{id}/policies | 
[**setConfigList**](RepositoryApi.md#setConfigList) | **PUT** /api/repository/v1/configs | 
[**setFault**](RepositoryApi.md#setFault) | **PUT** /api/repository/v1/configs/{name} | 
[**user**](RepositoryApi.md#user) | **GET** /api/repository/v1/register/user | 


<a name="addDatasetAssetSpecifications"></a>
# **addDatasetAssetSpecifications**
> JobModel addDatasetAssetSpecifications(id, assetModel)



Add an asset definiion to a dataset

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
AssetModel assetModel = new AssetModel(); // AssetModel | Asset definition to add to the dataset
try {
    JobModel result = apiInstance.addDatasetAssetSpecifications(id, assetModel);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#addDatasetAssetSpecifications");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **assetModel** | [**AssetModel**](AssetModel.md)| Asset definition to add to the dataset | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="addDatasetPolicyMember"></a>
# **addDatasetPolicyMember**
> PolicyResponse addDatasetPolicyMember(id, policyName, policyMember)



Adds a member to the specified policy for the snapshot

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String policyName = "policyName_example"; // String | The relevant policy
PolicyMemberRequest policyMember = new PolicyMemberRequest(); // PolicyMemberRequest | Snapshot to change the policy of
try {
    PolicyResponse result = apiInstance.addDatasetPolicyMember(id, policyName, policyMember);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#addDatasetPolicyMember");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **policyName** | **String**| The relevant policy | [enum: steward, custodian, ingester]
 **policyMember** | [**PolicyMemberRequest**](PolicyMemberRequest.md)| Snapshot to change the policy of | [optional]

### Return type

[**PolicyResponse**](PolicyResponse.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="addSnapshotPolicyMember"></a>
# **addSnapshotPolicyMember**
> PolicyResponse addSnapshotPolicyMember(id, policyName, policyMember)



Adds a member to the specified policy for the snapshot

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String policyName = "policyName_example"; // String | The relevant policy
PolicyMemberRequest policyMember = new PolicyMemberRequest(); // PolicyMemberRequest | Snapshot to change the policy of
try {
    PolicyResponse result = apiInstance.addSnapshotPolicyMember(id, policyName, policyMember);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#addSnapshotPolicyMember");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **policyName** | **String**| The relevant policy | [enum: reader, discoverer]
 **policyMember** | [**PolicyMemberRequest**](PolicyMemberRequest.md)| Snapshot to change the policy of | [optional]

### Return type

[**PolicyResponse**](PolicyResponse.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="applyDatasetDataDeletion"></a>
# **applyDatasetDataDeletion**
> JobModel applyDatasetDataDeletion(id, dataDeletionRequest)



Applies deletes to primary tabular data in a dataset

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
DataDeletionRequest dataDeletionRequest = new DataDeletionRequest(); // DataDeletionRequest | Description of the data in the dataset to delete
try {
    JobModel result = apiInstance.applyDatasetDataDeletion(id, dataDeletionRequest);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#applyDatasetDataDeletion");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **dataDeletionRequest** | [**DataDeletionRequest**](DataDeletionRequest.md)| Description of the data in the dataset to delete | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="bulkFileLoad"></a>
# **bulkFileLoad**
> JobModel bulkFileLoad(id, bulkFileLoad)



Load many files into the dataset file system; async returns a BulkLoadResultModel Note that this endpoint is not a single transaction. Some files may be loaded and others may fail. Each file load is atomic; the file will either be loaded into the dataset file system or it will not exist.

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
BulkLoadRequestModel bulkFileLoad = new BulkLoadRequestModel(); // BulkLoadRequestModel | Bulk file load request with file list in an external file. Load summary results are returned in the async response.
try {
    JobModel result = apiInstance.bulkFileLoad(id, bulkFileLoad);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#bulkFileLoad");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **bulkFileLoad** | [**BulkLoadRequestModel**](BulkLoadRequestModel.md)| Bulk file load request with file list in an external file. Load summary results are returned in the async response. | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="bulkFileLoadArray"></a>
# **bulkFileLoadArray**
> JobModel bulkFileLoadArray(id, bulkFileLoadArray)



Load many files into the dataset file system; async returns a BulkLoadArrayResultModel Note that this endpoint is not a single transaction. Some files may be loaded and others may fail. Each file load is atomic; the file will either be loaded into the dataset file system or it will not exist.

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
BulkLoadArrayRequestModel bulkFileLoadArray = new BulkLoadArrayRequestModel(); // BulkLoadArrayRequestModel | Bulk file load request with file list in the body of the request and load results returned in the async response.
try {
    JobModel result = apiInstance.bulkFileLoadArray(id, bulkFileLoadArray);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#bulkFileLoadArray");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **bulkFileLoadArray** | [**BulkLoadArrayRequestModel**](BulkLoadArrayRequestModel.md)| Bulk file load request with file list in the body of the request and load results returned in the async response. | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="bulkFileResultsDelete"></a>
# **bulkFileResultsDelete**
> JobModel bulkFileResultsDelete(id, loadtag, jobId)



Delete results from the bulk file load table of the dataset. If jobId is specified, then only the results for the loadTag plus that jobId are deleted. Otherwise, all results associated with the loadTag are deleted.

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String loadtag = "loadtag_example"; // String | a load tag
String jobId = "jobId_example"; // String | The job id associated with the load
try {
    JobModel result = apiInstance.bulkFileResultsDelete(id, loadtag, jobId);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#bulkFileResultsDelete");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **loadtag** | **String**| a load tag |
 **jobId** | **String**| The job id associated with the load | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="bulkFileResultsGet"></a>
# **bulkFileResultsGet**
> JobModel bulkFileResultsGet(id, loadtag, jobId, offset, limit)



Retrieve the results of a bulk file load. The results of each bulk load are stored in the dataset. They can be queried directly or retrieved with this paginated interface.

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String loadtag = "loadtag_example"; // String | a load tag
String jobId = "jobId_example"; // String | The job id associated with the load
Integer offset = 0; // Integer | The number of items to skip before starting to collect the result set.
Integer limit = 10; // Integer | The numbers of items to return.
try {
    JobModel result = apiInstance.bulkFileResultsGet(id, loadtag, jobId, offset, limit);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#bulkFileResultsGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **loadtag** | **String**| a load tag |
 **jobId** | **String**| The job id associated with the load | [optional]
 **offset** | **Integer**| The number of items to skip before starting to collect the result set. | [optional] [default to 0]
 **limit** | **Integer**| The numbers of items to return. | [optional] [default to 10]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="createDataset"></a>
# **createDataset**
> JobModel createDataset(dataset)



Create a new dataset asynchronously. The async result is DatasetSummaryModel.

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
DatasetRequestModel dataset = new DatasetRequestModel(); // DatasetRequestModel | Dataset to create
try {
    JobModel result = apiInstance.createDataset(dataset);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#createDataset");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset** | [**DatasetRequestModel**](DatasetRequestModel.md)| Dataset to create | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="createSnapshot"></a>
# **createSnapshot**
> JobModel createSnapshot(snapshot)



Create a new snapshot

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
SnapshotRequestModel snapshot = new SnapshotRequestModel(); // SnapshotRequestModel | Snapshot to create
try {
    JobModel result = apiInstance.createSnapshot(snapshot);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#createSnapshot");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **snapshot** | [**SnapshotRequestModel**](SnapshotRequestModel.md)| Snapshot to create | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="deleteDataset"></a>
# **deleteDataset**
> JobModel deleteDataset(id)



Delete a dataset by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    JobModel result = apiInstance.deleteDataset(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#deleteDataset");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="deleteDatasetPolicyMember"></a>
# **deleteDatasetPolicyMember**
> PolicyResponse deleteDatasetPolicyMember(id, policyName, memberEmail)



Removes the member from the specified policy for the snapshot

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String policyName = "policyName_example"; // String | The relevant policy
String memberEmail = "memberEmail_example"; // String | The email of the user to remove
try {
    PolicyResponse result = apiInstance.deleteDatasetPolicyMember(id, policyName, memberEmail);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#deleteDatasetPolicyMember");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **policyName** | **String**| The relevant policy | [enum: steward, custodian, ingester]
 **memberEmail** | **String**| The email of the user to remove |

### Return type

[**PolicyResponse**](PolicyResponse.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="deleteFile"></a>
# **deleteFile**
> JobModel deleteFile(id, fileid)



Hard delete of a file by id. The file is deleted even if it is in use by a dataset. Subsequent lookups will give not found errors. 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String fileid = "fileid_example"; // String | A file id
try {
    JobModel result = apiInstance.deleteFile(id, fileid);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#deleteFile");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **fileid** | **String**| A file id |

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="deleteJob"></a>
# **deleteJob**
> deleteJob(id)



Delete the job and data associated with it

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    apiInstance.deleteJob(id);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#deleteJob");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

null (empty response body)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="deleteSnapshot"></a>
# **deleteSnapshot**
> JobModel deleteSnapshot(id)



Delete a snapshot by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    JobModel result = apiInstance.deleteSnapshot(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#deleteSnapshot");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="deleteSnapshotPolicyMember"></a>
# **deleteSnapshotPolicyMember**
> PolicyResponse deleteSnapshotPolicyMember(id, policyName, memberEmail)



Adds a member to the specified policy for the snapshot

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String policyName = "policyName_example"; // String | The relevant policy
String memberEmail = "memberEmail_example"; // String | The email of the user to remove
try {
    PolicyResponse result = apiInstance.deleteSnapshotPolicyMember(id, policyName, memberEmail);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#deleteSnapshotPolicyMember");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **policyName** | **String**| The relevant policy | [enum: reader, discoverer]
 **memberEmail** | **String**| The email of the user to remove |

### Return type

[**PolicyResponse**](PolicyResponse.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="enumerateDatasets"></a>
# **enumerateDatasets**
> EnumerateDatasetModel enumerateDatasets(offset, limit, sort, direction, filter)



Returns a list of all of the datasets the caller has access to 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
Integer offset = 0; // Integer | The number of datasets to skip before when retrieving the next page
Integer limit = 10; // Integer | The numbers datasets to retrieve and return.
String sort = "created_date"; // String | The field to use for sorting.
String direction = "desc"; // String | The direction to sort.
String filter = "filter_example"; // String | Filter the results where this string is a case insensitive match in the name or description.
try {
    EnumerateDatasetModel result = apiInstance.enumerateDatasets(offset, limit, sort, direction, filter);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#enumerateDatasets");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **offset** | **Integer**| The number of datasets to skip before when retrieving the next page | [optional] [default to 0]
 **limit** | **Integer**| The numbers datasets to retrieve and return. | [optional] [default to 10]
 **sort** | **String**| The field to use for sorting. | [optional] [default to created_date] [enum: name, description, created_date]
 **direction** | **String**| The direction to sort. | [optional] [default to desc] [enum: asc, desc]
 **filter** | **String**| Filter the results where this string is a case insensitive match in the name or description. | [optional]

### Return type

[**EnumerateDatasetModel**](EnumerateDatasetModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="enumerateJobs"></a>
# **enumerateJobs**
> List&lt;JobModel&gt; enumerateJobs(offset, limit)



Returns a list of all of the jobs the caller has access to 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
Integer offset = 0; // Integer | The number of items to skip before starting to collect the result set.
Integer limit = 10; // Integer | The numbers of items to return.
try {
    List<JobModel> result = apiInstance.enumerateJobs(offset, limit);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#enumerateJobs");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **offset** | **Integer**| The number of items to skip before starting to collect the result set. | [optional] [default to 0]
 **limit** | **Integer**| The numbers of items to return. | [optional] [default to 10]

### Return type

[**List&lt;JobModel&gt;**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="enumerateSnapshots"></a>
# **enumerateSnapshots**
> EnumerateSnapshotModel enumerateSnapshots(offset, limit, sort, direction, filter)



Returns a list of all of the snapshots the caller has access to 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
Integer offset = 0; // Integer | The number of items to skip before starting to collect the result set.
Integer limit = 10; // Integer | The numbers of items to return.
String sort = "created_date"; // String | The field to use for sorting.
String direction = "desc"; // String | The direction to sort.
String filter = "filter_example"; // String | Filter the results where this string is a case insensitive match in the name or description.
try {
    EnumerateSnapshotModel result = apiInstance.enumerateSnapshots(offset, limit, sort, direction, filter);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#enumerateSnapshots");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **offset** | **Integer**| The number of items to skip before starting to collect the result set. | [optional] [default to 0]
 **limit** | **Integer**| The numbers of items to return. | [optional] [default to 10]
 **sort** | **String**| The field to use for sorting. | [optional] [default to created_date] [enum: name, description, created_date]
 **direction** | **String**| The direction to sort. | [optional] [default to desc] [enum: asc, desc]
 **filter** | **String**| Filter the results where this string is a case insensitive match in the name or description. | [optional]

### Return type

[**EnumerateSnapshotModel**](EnumerateSnapshotModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="getConfig"></a>
# **getConfig**
> ConfigModel getConfig(name)



Get one configuration

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String name = "name_example"; // String | name of the configuration to get
try {
    ConfigModel result = apiInstance.getConfig(name);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#getConfig");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **String**| name of the configuration to get |

### Return type

[**ConfigModel**](ConfigModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="getConfigList"></a>
# **getConfigList**
> ConfigListModel getConfigList()



Get all configurations

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
try {
    ConfigListModel result = apiInstance.getConfigList();
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#getConfigList");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**ConfigListModel**](ConfigListModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="ingestDataset"></a>
# **ingestDataset**
> JobModel ingestDataset(id, ingest)



Ingest data into a dataset table

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
IngestRequestModel ingest = new IngestRequestModel(); // IngestRequestModel | Ingest request
try {
    JobModel result = apiInstance.ingestDataset(id, ingest);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#ingestDataset");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **ingest** | [**IngestRequestModel**](IngestRequestModel.md)| Ingest request | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="ingestFile"></a>
# **ingestFile**
> JobModel ingestFile(id, ingestFile)



Ingest one file into the dataset file system; async returns a FileModel

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
FileLoadModel ingestFile = new FileLoadModel(); // FileLoadModel | Ingest file request
try {
    JobModel result = apiInstance.ingestFile(id, ingestFile);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#ingestFile");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **ingestFile** | [**FileLoadModel**](FileLoadModel.md)| Ingest file request | [optional]

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="lookupFileById"></a>
# **lookupFileById**
> FileModel lookupFileById(id, fileid, depth)



Lookup metadata for one file

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String fileid = "fileid_example"; // String | A file id
Integer depth = 0; // Integer | Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories
try {
    FileModel result = apiInstance.lookupFileById(id, fileid, depth);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#lookupFileById");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **fileid** | **String**| A file id |
 **depth** | **Integer**| Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories | [optional] [default to 0]

### Return type

[**FileModel**](FileModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="lookupFileByPath"></a>
# **lookupFileByPath**
> FileModel lookupFileByPath(id, path, depth)



Lookup metadata for one file

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String path = "path_example"; // String | URL-encoded full path to a file or directory
Integer depth = 0; // Integer | Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories
try {
    FileModel result = apiInstance.lookupFileByPath(id, path, depth);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#lookupFileByPath");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **path** | **String**| URL-encoded full path to a file or directory |
 **depth** | **Integer**| Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories | [optional] [default to 0]

### Return type

[**FileModel**](FileModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="lookupSnapshotFileById"></a>
# **lookupSnapshotFileById**
> FileModel lookupSnapshotFileById(id, fileid, depth)



Lookup metadata for one file

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String fileid = "fileid_example"; // String | A file id
Integer depth = 0; // Integer | Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories
try {
    FileModel result = apiInstance.lookupSnapshotFileById(id, fileid, depth);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#lookupSnapshotFileById");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **fileid** | **String**| A file id |
 **depth** | **Integer**| Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories | [optional] [default to 0]

### Return type

[**FileModel**](FileModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="lookupSnapshotFileByPath"></a>
# **lookupSnapshotFileByPath**
> FileModel lookupSnapshotFileByPath(id, path, depth)



Lookup metadata for one file

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String path = "path_example"; // String | URL-encoded full path to a file or directory
Integer depth = 0; // Integer | Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories
try {
    FileModel result = apiInstance.lookupSnapshotFileByPath(id, path, depth);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#lookupSnapshotFileByPath");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **path** | **String**| URL-encoded full path to a file or directory |
 **depth** | **Integer**| Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories | [optional] [default to 0]

### Return type

[**FileModel**](FileModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="removeDatasetAssetSpecifications"></a>
# **removeDatasetAssetSpecifications**
> JobModel removeDatasetAssetSpecifications(id, assetid)



Remove an asset definiion from a dataset

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
String assetid = "assetid_example"; // String | An asset id
try {
    JobModel result = apiInstance.removeDatasetAssetSpecifications(id, assetid);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#removeDatasetAssetSpecifications");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |
 **assetid** | **String**| An asset id |

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="resetConfig"></a>
# **resetConfig**
> resetConfig()



Reset the configuration to original settings

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
try {
    apiInstance.resetConfig();
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#resetConfig");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="retrieveDataset"></a>
# **retrieveDataset**
> DatasetModel retrieveDataset(id)



Retrieve a dataset by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    DatasetModel result = apiInstance.retrieveDataset(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#retrieveDataset");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**DatasetModel**](DatasetModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="retrieveDatasetPolicies"></a>
# **retrieveDatasetPolicies**
> PolicyResponse retrieveDatasetPolicies(id)



Retrieve the read and discover policies for the snapshot

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    PolicyResponse result = apiInstance.retrieveDatasetPolicies(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#retrieveDatasetPolicies");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**PolicyResponse**](PolicyResponse.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="retrieveJob"></a>
# **retrieveJob**
> JobModel retrieveJob(id)



Retrieve a job&#39;s status by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    JobModel result = apiInstance.retrieveJob(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#retrieveJob");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**JobModel**](JobModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="retrieveJobResult"></a>
# **retrieveJobResult**
> Object retrieveJobResult(id)



Retrieve a job&#39;s result by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    Object result = apiInstance.retrieveJobResult(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#retrieveJobResult");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

**Object**

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="retrieveSnapshot"></a>
# **retrieveSnapshot**
> SnapshotModel retrieveSnapshot(id)



Retrieve a snapshot by id

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    SnapshotModel result = apiInstance.retrieveSnapshot(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#retrieveSnapshot");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**SnapshotModel**](SnapshotModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="retrieveSnapshotPolicies"></a>
# **retrieveSnapshotPolicies**
> PolicyResponse retrieveSnapshotPolicies(id)



Retrieve the read and discover policies for the snapshot

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String id = "id_example"; // String | A UUID to used to identify an object in the repository
try {
    PolicyResponse result = apiInstance.retrieveSnapshotPolicies(id);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#retrieveSnapshotPolicies");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **String**| A UUID to used to identify an object in the repository |

### Return type

[**PolicyResponse**](PolicyResponse.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="setConfigList"></a>
# **setConfigList**
> ConfigListModel setConfigList(configModel)



Set the a group of configurations

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
ConfigGroupModel configModel = new ConfigGroupModel(); // ConfigGroupModel | 
try {
    ConfigListModel result = apiInstance.setConfigList(configModel);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#setConfigList");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **configModel** | [**ConfigGroupModel**](ConfigGroupModel.md)|  | [optional]

### Return type

[**ConfigListModel**](ConfigListModel.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="setFault"></a>
# **setFault**
> setFault(name, configEnable)



Enable or disable the named fault. Performing the put on a config that is not a fault is an error. 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
String name = "name_example"; // String | name of the configuration to
ConfigEnableModel configEnable = new ConfigEnableModel(); // ConfigEnableModel | 
try {
    apiInstance.setFault(name, configEnable);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#setFault");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **String**| name of the configuration to |
 **configEnable** | [**ConfigEnableModel**](ConfigEnableModel.md)|  | [optional]

### Return type

null (empty response body)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

<a name="user"></a>
# **user**
> UserStatusInfo user()



Returns whether the user is registered with terra 

### Example
```java
// Import classes:
//import bio.terra.datarepo.client.ApiClient;
//import bio.terra.datarepo.client.ApiException;
//import bio.terra.datarepo.client.Configuration;
//import bio.terra.datarepo.client.auth.*;
//import bio.terra.datarepo.api.RepositoryApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure OAuth2 access token for authorization: googleoauth
OAuth googleoauth = (OAuth) defaultClient.getAuthentication("googleoauth");
googleoauth.setAccessToken("YOUR ACCESS TOKEN");

RepositoryApi apiInstance = new RepositoryApi();
try {
    UserStatusInfo result = apiInstance.user();
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling RepositoryApi#user");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**UserStatusInfo**](UserStatusInfo.md)

### Authorization

[googleoauth](../README.md#googleoauth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

