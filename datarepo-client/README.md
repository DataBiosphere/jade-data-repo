# swagger-java-client

## Requirements

Building the API client library requires [Maven](https://maven.apache.org/) to be installed.

## Installation

To install the API client library to your local Maven repository, simply execute:

```shell
mvn install
```

To deploy it to a remote Maven repository instead, configure the settings of the repository and execute:

```shell
mvn deploy
```

Refer to the [official documentation](https://maven.apache.org/plugins/maven-deploy-plugin/usage.html) for more information.

### Maven users

Add this dependency to your project's POM:

```xml
<dependency>
    <groupId>io.swagger</groupId>
    <artifactId>swagger-java-client</artifactId>
    <version>1.0.0</version>
    <scope>compile</scope>
</dependency>
```

### Gradle users

Add this dependency to your project's build file:

```groovy
compile "io.swagger:swagger-java-client:1.0.0"
```

### Others

At first generate the JAR by executing:

    mvn package

Then manually install the following JARs:

* target/swagger-java-client-1.0.0.jar
* target/lib/*.jar

## Getting Started

Please follow the [installation](#installation) instruction and execute the following Java code:

```java

import bio.terra.datarepo.client.*;
import bio.terra.datarepo.client.auth.*;
import bio.terra.datarepo.model.*;
import bio.terra.datarepo.api.DataRepositoryServiceApi;

import java.io.File;
import java.util.*;

public class DataRepositoryServiceApiExample {

    public static void main(String[] args) {
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
    }
}

```

## Documentation for API Endpoints

All URIs are relative to *https://localhost*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DataRepositoryServiceApi* | [**getAccessURL**](docs/DataRepositoryServiceApi.md#getAccessURL) | **GET** /ga4gh/drs/v1/objects/{object_id}/access/{access_id} | Get a URL for fetching bytes.
*DataRepositoryServiceApi* | [**getObject**](docs/DataRepositoryServiceApi.md#getObject) | **GET** /ga4gh/drs/v1/objects/{object_id} | Get info about an &#x60;Object&#x60;.
*DataRepositoryServiceApi* | [**getServiceInfo**](docs/DataRepositoryServiceApi.md#getServiceInfo) | **GET** /ga4gh/drs/v1/service-info | Get information about this implementation.
*RepositoryApi* | [**addDatasetAssetSpecifications**](docs/RepositoryApi.md#addDatasetAssetSpecifications) | **POST** /api/repository/v1/datasets/{id}/assets | 
*RepositoryApi* | [**addDatasetPolicyMember**](docs/RepositoryApi.md#addDatasetPolicyMember) | **POST** /api/repository/v1/datasets/{id}/policies/{policyName}/members | 
*RepositoryApi* | [**addSnapshotPolicyMember**](docs/RepositoryApi.md#addSnapshotPolicyMember) | **POST** /api/repository/v1/snapshots/{id}/policies/{policyName}/members | 
*RepositoryApi* | [**applyDatasetDataDeletion**](docs/RepositoryApi.md#applyDatasetDataDeletion) | **POST** /api/repository/v1/datasets/{id}/deletes | 
*RepositoryApi* | [**bulkFileLoad**](docs/RepositoryApi.md#bulkFileLoad) | **POST** /api/repository/v1/datasets/{id}/files/bulk | 
*RepositoryApi* | [**bulkFileLoadArray**](docs/RepositoryApi.md#bulkFileLoadArray) | **POST** /api/repository/v1/datasets/{id}/files/bulk/array | 
*RepositoryApi* | [**bulkFileResultsDelete**](docs/RepositoryApi.md#bulkFileResultsDelete) | **DELETE** /api/repository/v1/datasets/{id}/files/bulk/{loadtag} | 
*RepositoryApi* | [**bulkFileResultsGet**](docs/RepositoryApi.md#bulkFileResultsGet) | **GET** /api/repository/v1/datasets/{id}/files/bulk/{loadtag} | 
*RepositoryApi* | [**createDataset**](docs/RepositoryApi.md#createDataset) | **POST** /api/repository/v1/datasets | 
*RepositoryApi* | [**createSnapshot**](docs/RepositoryApi.md#createSnapshot) | **POST** /api/repository/v1/snapshots | 
*RepositoryApi* | [**deleteDataset**](docs/RepositoryApi.md#deleteDataset) | **DELETE** /api/repository/v1/datasets/{id} | 
*RepositoryApi* | [**deleteDatasetPolicyMember**](docs/RepositoryApi.md#deleteDatasetPolicyMember) | **DELETE** /api/repository/v1/datasets/{id}/policies/{policyName}/members/{memberEmail} | 
*RepositoryApi* | [**deleteFile**](docs/RepositoryApi.md#deleteFile) | **DELETE** /api/repository/v1/datasets/{id}/files/{fileid} | 
*RepositoryApi* | [**deleteJob**](docs/RepositoryApi.md#deleteJob) | **DELETE** /api/repository/v1/jobs/{id} | 
*RepositoryApi* | [**deleteSnapshot**](docs/RepositoryApi.md#deleteSnapshot) | **DELETE** /api/repository/v1/snapshots/{id} | 
*RepositoryApi* | [**deleteSnapshotPolicyMember**](docs/RepositoryApi.md#deleteSnapshotPolicyMember) | **DELETE** /api/repository/v1/snapshots/{id}/policies/{policyName}/members/{memberEmail} | 
*RepositoryApi* | [**enumerateDatasets**](docs/RepositoryApi.md#enumerateDatasets) | **GET** /api/repository/v1/datasets | 
*RepositoryApi* | [**enumerateJobs**](docs/RepositoryApi.md#enumerateJobs) | **GET** /api/repository/v1/jobs | 
*RepositoryApi* | [**enumerateSnapshots**](docs/RepositoryApi.md#enumerateSnapshots) | **GET** /api/repository/v1/snapshots | 
*RepositoryApi* | [**getConfig**](docs/RepositoryApi.md#getConfig) | **GET** /api/repository/v1/configs/{name} | 
*RepositoryApi* | [**getConfigList**](docs/RepositoryApi.md#getConfigList) | **GET** /api/repository/v1/configs | 
*RepositoryApi* | [**ingestDataset**](docs/RepositoryApi.md#ingestDataset) | **POST** /api/repository/v1/datasets/{id}/ingest | 
*RepositoryApi* | [**ingestFile**](docs/RepositoryApi.md#ingestFile) | **POST** /api/repository/v1/datasets/{id}/files | 
*RepositoryApi* | [**lookupFileById**](docs/RepositoryApi.md#lookupFileById) | **GET** /api/repository/v1/datasets/{id}/files/{fileid} | 
*RepositoryApi* | [**lookupFileByPath**](docs/RepositoryApi.md#lookupFileByPath) | **GET** /api/repository/v1/datasets/{id}/filesystem/objects | 
*RepositoryApi* | [**lookupSnapshotFileById**](docs/RepositoryApi.md#lookupSnapshotFileById) | **GET** /api/repository/v1/snapshots/{id}/files/{fileid} | 
*RepositoryApi* | [**lookupSnapshotFileByPath**](docs/RepositoryApi.md#lookupSnapshotFileByPath) | **GET** /api/repository/v1/snapshots/{id}/filesystem/objects | 
*RepositoryApi* | [**removeDatasetAssetSpecifications**](docs/RepositoryApi.md#removeDatasetAssetSpecifications) | **DELETE** /api/repository/v1/datasets/{id}/assets/{assetid} | 
*RepositoryApi* | [**resetConfig**](docs/RepositoryApi.md#resetConfig) | **PUT** /api/repository/v1/configs/reset | 
*RepositoryApi* | [**retrieveDataset**](docs/RepositoryApi.md#retrieveDataset) | **GET** /api/repository/v1/datasets/{id} | 
*RepositoryApi* | [**retrieveDatasetPolicies**](docs/RepositoryApi.md#retrieveDatasetPolicies) | **GET** /api/repository/v1/datasets/{id}/policies | 
*RepositoryApi* | [**retrieveJob**](docs/RepositoryApi.md#retrieveJob) | **GET** /api/repository/v1/jobs/{id} | 
*RepositoryApi* | [**retrieveJobResult**](docs/RepositoryApi.md#retrieveJobResult) | **GET** /api/repository/v1/jobs/{id}/result | 
*RepositoryApi* | [**retrieveSnapshot**](docs/RepositoryApi.md#retrieveSnapshot) | **GET** /api/repository/v1/snapshots/{id} | 
*RepositoryApi* | [**retrieveSnapshotPolicies**](docs/RepositoryApi.md#retrieveSnapshotPolicies) | **GET** /api/repository/v1/snapshots/{id}/policies | 
*RepositoryApi* | [**setConfigList**](docs/RepositoryApi.md#setConfigList) | **PUT** /api/repository/v1/configs | 
*RepositoryApi* | [**setFault**](docs/RepositoryApi.md#setFault) | **PUT** /api/repository/v1/configs/{name} | 
*RepositoryApi* | [**user**](docs/RepositoryApi.md#user) | **GET** /api/repository/v1/register/user | 
*ResourcesApi* | [**createProfile**](docs/ResourcesApi.md#createProfile) | **POST** /api/resources/v1/profiles | 
*ResourcesApi* | [**deleteProfile**](docs/ResourcesApi.md#deleteProfile) | **DELETE** /api/resources/v1/profiles/{id} | 
*ResourcesApi* | [**enumerateProfiles**](docs/ResourcesApi.md#enumerateProfiles) | **GET** /api/resources/v1/profiles | 
*ResourcesApi* | [**retrieveProfile**](docs/ResourcesApi.md#retrieveProfile) | **GET** /api/resources/v1/profiles/{id} | 
*UnauthenticatedApi* | [**retrieveRepositoryConfig**](docs/UnauthenticatedApi.md#retrieveRepositoryConfig) | **GET** /configuration | 
*UnauthenticatedApi* | [**serviceStatus**](docs/UnauthenticatedApi.md#serviceStatus) | **GET** /status | 
*UnauthenticatedApi* | [**shutdownRequest**](docs/UnauthenticatedApi.md#shutdownRequest) | **GET** /shutdown | 


## Documentation for Models

 - [AssetModel](docs/AssetModel.md)
 - [AssetTableModel](docs/AssetTableModel.md)
 - [BillingProfileModel](docs/BillingProfileModel.md)
 - [BillingProfileRequestModel](docs/BillingProfileRequestModel.md)
 - [BulkLoadArrayRequestModel](docs/BulkLoadArrayRequestModel.md)
 - [BulkLoadArrayResultModel](docs/BulkLoadArrayResultModel.md)
 - [BulkLoadFileModel](docs/BulkLoadFileModel.md)
 - [BulkLoadFileResultModel](docs/BulkLoadFileResultModel.md)
 - [BulkLoadFileState](docs/BulkLoadFileState.md)
 - [BulkLoadHistoryModel](docs/BulkLoadHistoryModel.md)
 - [BulkLoadRequestModel](docs/BulkLoadRequestModel.md)
 - [BulkLoadResultModel](docs/BulkLoadResultModel.md)
 - [ColumnModel](docs/ColumnModel.md)
 - [ConfigEnableModel](docs/ConfigEnableModel.md)
 - [ConfigFaultCountedModel](docs/ConfigFaultCountedModel.md)
 - [ConfigFaultModel](docs/ConfigFaultModel.md)
 - [ConfigGroupModel](docs/ConfigGroupModel.md)
 - [ConfigListModel](docs/ConfigListModel.md)
 - [ConfigModel](docs/ConfigModel.md)
 - [ConfigParameterModel](docs/ConfigParameterModel.md)
 - [DRSAccessMethod](docs/DRSAccessMethod.md)
 - [DRSAccessURL](docs/DRSAccessURL.md)
 - [DRSChecksum](docs/DRSChecksum.md)
 - [DRSContentsObject](docs/DRSContentsObject.md)
 - [DRSError](docs/DRSError.md)
 - [DRSObject](docs/DRSObject.md)
 - [DRSServiceInfo](docs/DRSServiceInfo.md)
 - [DataDeletionGcsFileModel](docs/DataDeletionGcsFileModel.md)
 - [DataDeletionRequest](docs/DataDeletionRequest.md)
 - [DataDeletionTableModel](docs/DataDeletionTableModel.md)
 - [DatasetModel](docs/DatasetModel.md)
 - [DatasetRequestModel](docs/DatasetRequestModel.md)
 - [DatasetSpecificationModel](docs/DatasetSpecificationModel.md)
 - [DatasetSummaryModel](docs/DatasetSummaryModel.md)
 - [DatePartitionOptionsModel](docs/DatePartitionOptionsModel.md)
 - [DeleteResponseModel](docs/DeleteResponseModel.md)
 - [DirectoryDetailModel](docs/DirectoryDetailModel.md)
 - [EnumerateBillingProfileModel](docs/EnumerateBillingProfileModel.md)
 - [EnumerateDatasetModel](docs/EnumerateDatasetModel.md)
 - [EnumerateSnapshotModel](docs/EnumerateSnapshotModel.md)
 - [ErrorModel](docs/ErrorModel.md)
 - [FileDetailModel](docs/FileDetailModel.md)
 - [FileLoadModel](docs/FileLoadModel.md)
 - [FileModel](docs/FileModel.md)
 - [FileModelType](docs/FileModelType.md)
 - [IngestRequestModel](docs/IngestRequestModel.md)
 - [IngestResponseModel](docs/IngestResponseModel.md)
 - [IntPartitionOptionsModel](docs/IntPartitionOptionsModel.md)
 - [JobModel](docs/JobModel.md)
 - [PolicyMemberRequest](docs/PolicyMemberRequest.md)
 - [PolicyModel](docs/PolicyModel.md)
 - [PolicyResponse](docs/PolicyResponse.md)
 - [RelationshipModel](docs/RelationshipModel.md)
 - [RelationshipTermModel](docs/RelationshipTermModel.md)
 - [RepositoryConfigurationModel](docs/RepositoryConfigurationModel.md)
 - [SnapshotModel](docs/SnapshotModel.md)
 - [SnapshotRequestAssetModel](docs/SnapshotRequestAssetModel.md)
 - [SnapshotRequestContentsModel](docs/SnapshotRequestContentsModel.md)
 - [SnapshotRequestModel](docs/SnapshotRequestModel.md)
 - [SnapshotRequestQueryModel](docs/SnapshotRequestQueryModel.md)
 - [SnapshotRequestRowIdModel](docs/SnapshotRequestRowIdModel.md)
 - [SnapshotRequestRowIdTableModel](docs/SnapshotRequestRowIdTableModel.md)
 - [SnapshotSourceModel](docs/SnapshotSourceModel.md)
 - [SnapshotSummaryModel](docs/SnapshotSummaryModel.md)
 - [TableModel](docs/TableModel.md)
 - [UserStatusInfo](docs/UserStatusInfo.md)


## Documentation for Authorization

Authentication schemes defined for the API:
### authToken

- **Type**: API key
- **API key parameter name**: Authorization
- **Location**: HTTP header

### googleoauth

- **Type**: OAuth
- **Flow**: implicit
- **Authorization URL**: https://accounts.google.com/o/oauth2/auth
- **Scopes**: 
  - openid: open id authorization
  - email: email authorization
  - profile: profile authorization


## Recommendation

It's recommended to create an instance of `ApiClient` per thread in a multithreaded environment to avoid any potential issues.

## Author



