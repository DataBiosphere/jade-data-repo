package bio.terra.datarepo.api;

import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiResponse;
import bio.terra.datarepo.client.Configuration;
import bio.terra.datarepo.client.Pair;

import javax.ws.rs.core.GenericType;

import bio.terra.datarepo.model.AssetModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadRequestModel;
import bio.terra.datarepo.model.ConfigEnableModel;
import bio.terra.datarepo.model.ConfigGroupModel;
import bio.terra.datarepo.model.ConfigListModel;
import bio.terra.datarepo.model.ConfigModel;
import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.EnumerateDatasetModel;
import bio.terra.datarepo.model.EnumerateSnapshotModel;
import bio.terra.datarepo.model.ErrorModel;
import bio.terra.datarepo.model.FileLoadModel;
import bio.terra.datarepo.model.FileModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotRequestModel;
import bio.terra.datarepo.model.UserStatusInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class RepositoryApi {
  private ApiClient apiClient;

  public RepositoryApi() {
    this(Configuration.getDefaultApiClient());
  }

  public RepositoryApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  /**
   * 
   * Add an asset definiion to a dataset
   * @param id A UUID to used to identify an object in the repository (required)
   * @param assetModel Asset definition to add to the dataset (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel addDatasetAssetSpecifications(String id, AssetModel assetModel) throws ApiException {
    return addDatasetAssetSpecificationsWithHttpInfo(id, assetModel).getData();
      }

  /**
   * 
   * Add an asset definiion to a dataset
   * @param id A UUID to used to identify an object in the repository (required)
   * @param assetModel Asset definition to add to the dataset (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> addDatasetAssetSpecificationsWithHttpInfo(String id, AssetModel assetModel) throws ApiException {
    Object localVarPostBody = assetModel;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling addDatasetAssetSpecifications");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/assets"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Adds a member to the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param policyMember Snapshot to change the policy of (optional)
   * @return PolicyResponse
   * @throws ApiException if fails to make API call
   */
  public PolicyResponse addDatasetPolicyMember(String id, String policyName, PolicyMemberRequest policyMember) throws ApiException {
    return addDatasetPolicyMemberWithHttpInfo(id, policyName, policyMember).getData();
      }

  /**
   * 
   * Adds a member to the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param policyMember Snapshot to change the policy of (optional)
   * @return ApiResponse&lt;PolicyResponse&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<PolicyResponse> addDatasetPolicyMemberWithHttpInfo(String id, String policyName, PolicyMemberRequest policyMember) throws ApiException {
    Object localVarPostBody = policyMember;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling addDatasetPolicyMember");
    }
    
    // verify the required parameter 'policyName' is set
    if (policyName == null) {
      throw new ApiException(400, "Missing the required parameter 'policyName' when calling addDatasetPolicyMember");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/policies/{policyName}/members"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "policyName" + "\\}", apiClient.escapeString(policyName.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<PolicyResponse> localVarReturnType = new GenericType<PolicyResponse>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Adds a member to the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param policyMember Snapshot to change the policy of (optional)
   * @return PolicyResponse
   * @throws ApiException if fails to make API call
   */
  public PolicyResponse addSnapshotPolicyMember(String id, String policyName, PolicyMemberRequest policyMember) throws ApiException {
    return addSnapshotPolicyMemberWithHttpInfo(id, policyName, policyMember).getData();
      }

  /**
   * 
   * Adds a member to the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param policyMember Snapshot to change the policy of (optional)
   * @return ApiResponse&lt;PolicyResponse&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<PolicyResponse> addSnapshotPolicyMemberWithHttpInfo(String id, String policyName, PolicyMemberRequest policyMember) throws ApiException {
    Object localVarPostBody = policyMember;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling addSnapshotPolicyMember");
    }
    
    // verify the required parameter 'policyName' is set
    if (policyName == null) {
      throw new ApiException(400, "Missing the required parameter 'policyName' when calling addSnapshotPolicyMember");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots/{id}/policies/{policyName}/members"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "policyName" + "\\}", apiClient.escapeString(policyName.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<PolicyResponse> localVarReturnType = new GenericType<PolicyResponse>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Applies deletes to primary tabular data in a dataset
   * @param id A UUID to used to identify an object in the repository (required)
   * @param dataDeletionRequest Description of the data in the dataset to delete (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel applyDatasetDataDeletion(String id, DataDeletionRequest dataDeletionRequest) throws ApiException {
    return applyDatasetDataDeletionWithHttpInfo(id, dataDeletionRequest).getData();
      }

  /**
   * 
   * Applies deletes to primary tabular data in a dataset
   * @param id A UUID to used to identify an object in the repository (required)
   * @param dataDeletionRequest Description of the data in the dataset to delete (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> applyDatasetDataDeletionWithHttpInfo(String id, DataDeletionRequest dataDeletionRequest) throws ApiException {
    Object localVarPostBody = dataDeletionRequest;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling applyDatasetDataDeletion");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/deletes"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Load many files into the dataset file system; async returns a BulkLoadResultModel Note that this endpoint is not a single transaction. Some files may be loaded and others may fail. Each file load is atomic; the file will either be loaded into the dataset file system or it will not exist.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param bulkFileLoad Bulk file load request with file list in an external file. Load summary results are returned in the async response. (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel bulkFileLoad(String id, BulkLoadRequestModel bulkFileLoad) throws ApiException {
    return bulkFileLoadWithHttpInfo(id, bulkFileLoad).getData();
      }

  /**
   * 
   * Load many files into the dataset file system; async returns a BulkLoadResultModel Note that this endpoint is not a single transaction. Some files may be loaded and others may fail. Each file load is atomic; the file will either be loaded into the dataset file system or it will not exist.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param bulkFileLoad Bulk file load request with file list in an external file. Load summary results are returned in the async response. (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> bulkFileLoadWithHttpInfo(String id, BulkLoadRequestModel bulkFileLoad) throws ApiException {
    Object localVarPostBody = bulkFileLoad;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling bulkFileLoad");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/files/bulk"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Load many files into the dataset file system; async returns a BulkLoadArrayResultModel Note that this endpoint is not a single transaction. Some files may be loaded and others may fail. Each file load is atomic; the file will either be loaded into the dataset file system or it will not exist.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param bulkFileLoadArray Bulk file load request with file list in the body of the request and load results returned in the async response. (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel bulkFileLoadArray(String id, BulkLoadArrayRequestModel bulkFileLoadArray) throws ApiException {
    return bulkFileLoadArrayWithHttpInfo(id, bulkFileLoadArray).getData();
      }

  /**
   * 
   * Load many files into the dataset file system; async returns a BulkLoadArrayResultModel Note that this endpoint is not a single transaction. Some files may be loaded and others may fail. Each file load is atomic; the file will either be loaded into the dataset file system or it will not exist.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param bulkFileLoadArray Bulk file load request with file list in the body of the request and load results returned in the async response. (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> bulkFileLoadArrayWithHttpInfo(String id, BulkLoadArrayRequestModel bulkFileLoadArray) throws ApiException {
    Object localVarPostBody = bulkFileLoadArray;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling bulkFileLoadArray");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/files/bulk/array"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Delete results from the bulk file load table of the dataset. If jobId is specified, then only the results for the loadTag plus that jobId are deleted. Otherwise, all results associated with the loadTag are deleted.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param loadtag a load tag (required)
   * @param jobId The job id associated with the load (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel bulkFileResultsDelete(String id, String loadtag, String jobId) throws ApiException {
    return bulkFileResultsDeleteWithHttpInfo(id, loadtag, jobId).getData();
      }

  /**
   * 
   * Delete results from the bulk file load table of the dataset. If jobId is specified, then only the results for the loadTag plus that jobId are deleted. Otherwise, all results associated with the loadTag are deleted.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param loadtag a load tag (required)
   * @param jobId The job id associated with the load (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> bulkFileResultsDeleteWithHttpInfo(String id, String loadtag, String jobId) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling bulkFileResultsDelete");
    }
    
    // verify the required parameter 'loadtag' is set
    if (loadtag == null) {
      throw new ApiException(400, "Missing the required parameter 'loadtag' when calling bulkFileResultsDelete");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/files/bulk/{loadtag}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "loadtag" + "\\}", apiClient.escapeString(loadtag.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "jobId", jobId));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Retrieve the results of a bulk file load. The results of each bulk load are stored in the dataset. They can be queried directly or retrieved with this paginated interface.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param loadtag a load tag (required)
   * @param jobId The job id associated with the load (optional)
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel bulkFileResultsGet(String id, String loadtag, String jobId, Integer offset, Integer limit) throws ApiException {
    return bulkFileResultsGetWithHttpInfo(id, loadtag, jobId, offset, limit).getData();
      }

  /**
   * 
   * Retrieve the results of a bulk file load. The results of each bulk load are stored in the dataset. They can be queried directly or retrieved with this paginated interface.
   * @param id A UUID to used to identify an object in the repository (required)
   * @param loadtag a load tag (required)
   * @param jobId The job id associated with the load (optional)
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> bulkFileResultsGetWithHttpInfo(String id, String loadtag, String jobId, Integer offset, Integer limit) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling bulkFileResultsGet");
    }
    
    // verify the required parameter 'loadtag' is set
    if (loadtag == null) {
      throw new ApiException(400, "Missing the required parameter 'loadtag' when calling bulkFileResultsGet");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/files/bulk/{loadtag}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "loadtag" + "\\}", apiClient.escapeString(loadtag.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "jobId", jobId));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "offset", offset));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "limit", limit));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Create a new dataset asynchronously. The async result is DatasetSummaryModel.
   * @param dataset Dataset to create (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel createDataset(DatasetRequestModel dataset) throws ApiException {
    return createDatasetWithHttpInfo(dataset).getData();
      }

  /**
   * 
   * Create a new dataset asynchronously. The async result is DatasetSummaryModel.
   * @param dataset Dataset to create (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> createDatasetWithHttpInfo(DatasetRequestModel dataset) throws ApiException {
    Object localVarPostBody = dataset;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Create a new snapshot
   * @param snapshot Snapshot to create (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel createSnapshot(SnapshotRequestModel snapshot) throws ApiException {
    return createSnapshotWithHttpInfo(snapshot).getData();
      }

  /**
   * 
   * Create a new snapshot
   * @param snapshot Snapshot to create (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> createSnapshotWithHttpInfo(SnapshotRequestModel snapshot) throws ApiException {
    Object localVarPostBody = snapshot;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Delete a dataset by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel deleteDataset(String id) throws ApiException {
    return deleteDatasetWithHttpInfo(id).getData();
      }

  /**
   * 
   * Delete a dataset by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> deleteDatasetWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling deleteDataset");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Removes the member from the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param memberEmail The email of the user to remove (required)
   * @return PolicyResponse
   * @throws ApiException if fails to make API call
   */
  public PolicyResponse deleteDatasetPolicyMember(String id, String policyName, String memberEmail) throws ApiException {
    return deleteDatasetPolicyMemberWithHttpInfo(id, policyName, memberEmail).getData();
      }

  /**
   * 
   * Removes the member from the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param memberEmail The email of the user to remove (required)
   * @return ApiResponse&lt;PolicyResponse&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<PolicyResponse> deleteDatasetPolicyMemberWithHttpInfo(String id, String policyName, String memberEmail) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling deleteDatasetPolicyMember");
    }
    
    // verify the required parameter 'policyName' is set
    if (policyName == null) {
      throw new ApiException(400, "Missing the required parameter 'policyName' when calling deleteDatasetPolicyMember");
    }
    
    // verify the required parameter 'memberEmail' is set
    if (memberEmail == null) {
      throw new ApiException(400, "Missing the required parameter 'memberEmail' when calling deleteDatasetPolicyMember");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/policies/{policyName}/members/{memberEmail}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "policyName" + "\\}", apiClient.escapeString(policyName.toString()))
      .replaceAll("\\{" + "memberEmail" + "\\}", apiClient.escapeString(memberEmail.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<PolicyResponse> localVarReturnType = new GenericType<PolicyResponse>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Hard delete of a file by id. The file is deleted even if it is in use by a dataset. Subsequent lookups will give not found errors. 
   * @param id A UUID to used to identify an object in the repository (required)
   * @param fileid A file id (required)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel deleteFile(String id, String fileid) throws ApiException {
    return deleteFileWithHttpInfo(id, fileid).getData();
      }

  /**
   * 
   * Hard delete of a file by id. The file is deleted even if it is in use by a dataset. Subsequent lookups will give not found errors. 
   * @param id A UUID to used to identify an object in the repository (required)
   * @param fileid A file id (required)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> deleteFileWithHttpInfo(String id, String fileid) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling deleteFile");
    }
    
    // verify the required parameter 'fileid' is set
    if (fileid == null) {
      throw new ApiException(400, "Missing the required parameter 'fileid' when calling deleteFile");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/files/{fileid}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "fileid" + "\\}", apiClient.escapeString(fileid.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Delete the job and data associated with it
   * @param id A UUID to used to identify an object in the repository (required)
   * @throws ApiException if fails to make API call
   */
  public void deleteJob(String id) throws ApiException {

    deleteJobWithHttpInfo(id);
  }

  /**
   * 
   * Delete the job and data associated with it
   * @param id A UUID to used to identify an object in the repository (required)
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<Void> deleteJobWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling deleteJob");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/jobs/{id}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };


    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, null);
  }
  /**
   * 
   * Delete a snapshot by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel deleteSnapshot(String id) throws ApiException {
    return deleteSnapshotWithHttpInfo(id).getData();
      }

  /**
   * 
   * Delete a snapshot by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> deleteSnapshotWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling deleteSnapshot");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots/{id}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Adds a member to the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param memberEmail The email of the user to remove (required)
   * @return PolicyResponse
   * @throws ApiException if fails to make API call
   */
  public PolicyResponse deleteSnapshotPolicyMember(String id, String policyName, String memberEmail) throws ApiException {
    return deleteSnapshotPolicyMemberWithHttpInfo(id, policyName, memberEmail).getData();
      }

  /**
   * 
   * Adds a member to the specified policy for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @param policyName The relevant policy (required)
   * @param memberEmail The email of the user to remove (required)
   * @return ApiResponse&lt;PolicyResponse&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<PolicyResponse> deleteSnapshotPolicyMemberWithHttpInfo(String id, String policyName, String memberEmail) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling deleteSnapshotPolicyMember");
    }
    
    // verify the required parameter 'policyName' is set
    if (policyName == null) {
      throw new ApiException(400, "Missing the required parameter 'policyName' when calling deleteSnapshotPolicyMember");
    }
    
    // verify the required parameter 'memberEmail' is set
    if (memberEmail == null) {
      throw new ApiException(400, "Missing the required parameter 'memberEmail' when calling deleteSnapshotPolicyMember");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots/{id}/policies/{policyName}/members/{memberEmail}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "policyName" + "\\}", apiClient.escapeString(policyName.toString()))
      .replaceAll("\\{" + "memberEmail" + "\\}", apiClient.escapeString(memberEmail.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<PolicyResponse> localVarReturnType = new GenericType<PolicyResponse>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Returns a list of all of the datasets the caller has access to 
   * @param offset The number of datasets to skip before when retrieving the next page (optional, default to 0)
   * @param limit The numbers datasets to retrieve and return. (optional, default to 10)
   * @param sort The field to use for sorting. (optional, default to created_date)
   * @param direction The direction to sort. (optional, default to desc)
   * @param filter Filter the results where this string is a case insensitive match in the name or description. (optional)
   * @return EnumerateDatasetModel
   * @throws ApiException if fails to make API call
   */
  public EnumerateDatasetModel enumerateDatasets(Integer offset, Integer limit, String sort, String direction, String filter) throws ApiException {
    return enumerateDatasetsWithHttpInfo(offset, limit, sort, direction, filter).getData();
      }

  /**
   * 
   * Returns a list of all of the datasets the caller has access to 
   * @param offset The number of datasets to skip before when retrieving the next page (optional, default to 0)
   * @param limit The numbers datasets to retrieve and return. (optional, default to 10)
   * @param sort The field to use for sorting. (optional, default to created_date)
   * @param direction The direction to sort. (optional, default to desc)
   * @param filter Filter the results where this string is a case insensitive match in the name or description. (optional)
   * @return ApiResponse&lt;EnumerateDatasetModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<EnumerateDatasetModel> enumerateDatasetsWithHttpInfo(Integer offset, Integer limit, String sort, String direction, String filter) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "offset", offset));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "limit", limit));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "sort", sort));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "direction", direction));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "filter", filter));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<EnumerateDatasetModel> localVarReturnType = new GenericType<EnumerateDatasetModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Returns a list of all of the jobs the caller has access to 
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @return List&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public List<JobModel> enumerateJobs(Integer offset, Integer limit) throws ApiException {
    return enumerateJobsWithHttpInfo(offset, limit).getData();
      }

  /**
   * 
   * Returns a list of all of the jobs the caller has access to 
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @return ApiResponse&lt;List&lt;JobModel&gt;&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<List<JobModel>> enumerateJobsWithHttpInfo(Integer offset, Integer limit) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/jobs";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "offset", offset));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "limit", limit));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<List<JobModel>> localVarReturnType = new GenericType<List<JobModel>>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Returns a list of all of the snapshots the caller has access to 
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @param sort The field to use for sorting. (optional, default to created_date)
   * @param direction The direction to sort. (optional, default to desc)
   * @param filter Filter the results where this string is a case insensitive match in the name or description. (optional)
   * @return EnumerateSnapshotModel
   * @throws ApiException if fails to make API call
   */
  public EnumerateSnapshotModel enumerateSnapshots(Integer offset, Integer limit, String sort, String direction, String filter) throws ApiException {
    return enumerateSnapshotsWithHttpInfo(offset, limit, sort, direction, filter).getData();
      }

  /**
   * 
   * Returns a list of all of the snapshots the caller has access to 
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @param sort The field to use for sorting. (optional, default to created_date)
   * @param direction The direction to sort. (optional, default to desc)
   * @param filter Filter the results where this string is a case insensitive match in the name or description. (optional)
   * @return ApiResponse&lt;EnumerateSnapshotModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<EnumerateSnapshotModel> enumerateSnapshotsWithHttpInfo(Integer offset, Integer limit, String sort, String direction, String filter) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "offset", offset));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "limit", limit));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "sort", sort));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "direction", direction));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "filter", filter));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<EnumerateSnapshotModel> localVarReturnType = new GenericType<EnumerateSnapshotModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Get one configuration
   * @param name name of the configuration to get (required)
   * @return ConfigModel
   * @throws ApiException if fails to make API call
   */
  public ConfigModel getConfig(String name) throws ApiException {
    return getConfigWithHttpInfo(name).getData();
      }

  /**
   * 
   * Get one configuration
   * @param name name of the configuration to get (required)
   * @return ApiResponse&lt;ConfigModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<ConfigModel> getConfigWithHttpInfo(String name) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'name' is set
    if (name == null) {
      throw new ApiException(400, "Missing the required parameter 'name' when calling getConfig");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/configs/{name}"
      .replaceAll("\\{" + "name" + "\\}", apiClient.escapeString(name.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<ConfigModel> localVarReturnType = new GenericType<ConfigModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Get all configurations
   * @return ConfigListModel
   * @throws ApiException if fails to make API call
   */
  public ConfigListModel getConfigList() throws ApiException {
    return getConfigListWithHttpInfo().getData();
      }

  /**
   * 
   * Get all configurations
   * @return ApiResponse&lt;ConfigListModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<ConfigListModel> getConfigListWithHttpInfo() throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/configs";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<ConfigListModel> localVarReturnType = new GenericType<ConfigListModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Ingest data into a dataset table
   * @param id A UUID to used to identify an object in the repository (required)
   * @param ingest Ingest request (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel ingestDataset(String id, IngestRequestModel ingest) throws ApiException {
    return ingestDatasetWithHttpInfo(id, ingest).getData();
      }

  /**
   * 
   * Ingest data into a dataset table
   * @param id A UUID to used to identify an object in the repository (required)
   * @param ingest Ingest request (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> ingestDatasetWithHttpInfo(String id, IngestRequestModel ingest) throws ApiException {
    Object localVarPostBody = ingest;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling ingestDataset");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/ingest"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Ingest one file into the dataset file system; async returns a FileModel
   * @param id A UUID to used to identify an object in the repository (required)
   * @param ingestFile Ingest file request (optional)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel ingestFile(String id, FileLoadModel ingestFile) throws ApiException {
    return ingestFileWithHttpInfo(id, ingestFile).getData();
      }

  /**
   * 
   * Ingest one file into the dataset file system; async returns a FileModel
   * @param id A UUID to used to identify an object in the repository (required)
   * @param ingestFile Ingest file request (optional)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> ingestFileWithHttpInfo(String id, FileLoadModel ingestFile) throws ApiException {
    Object localVarPostBody = ingestFile;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling ingestFile");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/files"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param fileid A file id (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return FileModel
   * @throws ApiException if fails to make API call
   */
  public FileModel lookupFileById(String id, String fileid, Integer depth) throws ApiException {
    return lookupFileByIdWithHttpInfo(id, fileid, depth).getData();
      }

  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param fileid A file id (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return ApiResponse&lt;FileModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<FileModel> lookupFileByIdWithHttpInfo(String id, String fileid, Integer depth) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling lookupFileById");
    }
    
    // verify the required parameter 'fileid' is set
    if (fileid == null) {
      throw new ApiException(400, "Missing the required parameter 'fileid' when calling lookupFileById");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/files/{fileid}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "fileid" + "\\}", apiClient.escapeString(fileid.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "depth", depth));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<FileModel> localVarReturnType = new GenericType<FileModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param path URL-encoded full path to a file or directory (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return FileModel
   * @throws ApiException if fails to make API call
   */
  public FileModel lookupFileByPath(String id, String path, Integer depth) throws ApiException {
    return lookupFileByPathWithHttpInfo(id, path, depth).getData();
      }

  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param path URL-encoded full path to a file or directory (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return ApiResponse&lt;FileModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<FileModel> lookupFileByPathWithHttpInfo(String id, String path, Integer depth) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling lookupFileByPath");
    }
    
    // verify the required parameter 'path' is set
    if (path == null) {
      throw new ApiException(400, "Missing the required parameter 'path' when calling lookupFileByPath");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/filesystem/objects"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "path", path));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "depth", depth));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<FileModel> localVarReturnType = new GenericType<FileModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param fileid A file id (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return FileModel
   * @throws ApiException if fails to make API call
   */
  public FileModel lookupSnapshotFileById(String id, String fileid, Integer depth) throws ApiException {
    return lookupSnapshotFileByIdWithHttpInfo(id, fileid, depth).getData();
      }

  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param fileid A file id (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return ApiResponse&lt;FileModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<FileModel> lookupSnapshotFileByIdWithHttpInfo(String id, String fileid, Integer depth) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling lookupSnapshotFileById");
    }
    
    // verify the required parameter 'fileid' is set
    if (fileid == null) {
      throw new ApiException(400, "Missing the required parameter 'fileid' when calling lookupSnapshotFileById");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots/{id}/files/{fileid}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "fileid" + "\\}", apiClient.escapeString(fileid.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "depth", depth));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<FileModel> localVarReturnType = new GenericType<FileModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param path URL-encoded full path to a file or directory (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return FileModel
   * @throws ApiException if fails to make API call
   */
  public FileModel lookupSnapshotFileByPath(String id, String path, Integer depth) throws ApiException {
    return lookupSnapshotFileByPathWithHttpInfo(id, path, depth).getData();
      }

  /**
   * 
   * Lookup metadata for one file
   * @param id A UUID to used to identify an object in the repository (required)
   * @param path URL-encoded full path to a file or directory (required)
   * @param depth Enumeration depth; -1 means fully expand; 0 means no expansion; 1..N expands that many subdirectories (optional, default to 0)
   * @return ApiResponse&lt;FileModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<FileModel> lookupSnapshotFileByPathWithHttpInfo(String id, String path, Integer depth) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling lookupSnapshotFileByPath");
    }
    
    // verify the required parameter 'path' is set
    if (path == null) {
      throw new ApiException(400, "Missing the required parameter 'path' when calling lookupSnapshotFileByPath");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots/{id}/filesystem/objects"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "path", path));
    localVarQueryParams.addAll(apiClient.parameterToPairs("", "depth", depth));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<FileModel> localVarReturnType = new GenericType<FileModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Remove an asset definiion from a dataset
   * @param id A UUID to used to identify an object in the repository (required)
   * @param assetid An asset id (required)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel removeDatasetAssetSpecifications(String id, String assetid) throws ApiException {
    return removeDatasetAssetSpecificationsWithHttpInfo(id, assetid).getData();
      }

  /**
   * 
   * Remove an asset definiion from a dataset
   * @param id A UUID to used to identify an object in the repository (required)
   * @param assetid An asset id (required)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> removeDatasetAssetSpecificationsWithHttpInfo(String id, String assetid) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling removeDatasetAssetSpecifications");
    }
    
    // verify the required parameter 'assetid' is set
    if (assetid == null) {
      throw new ApiException(400, "Missing the required parameter 'assetid' when calling removeDatasetAssetSpecifications");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/assets/{assetid}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()))
      .replaceAll("\\{" + "assetid" + "\\}", apiClient.escapeString(assetid.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Reset the configuration to original settings
   * @throws ApiException if fails to make API call
   */
  public void resetConfig() throws ApiException {

    resetConfigWithHttpInfo();
  }

  /**
   * 
   * Reset the configuration to original settings
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<Void> resetConfigWithHttpInfo() throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/configs/reset";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };


    return apiClient.invokeAPI(localVarPath, "PUT", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, null);
  }
  /**
   * 
   * Retrieve a dataset by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return DatasetModel
   * @throws ApiException if fails to make API call
   */
  public DatasetModel retrieveDataset(String id) throws ApiException {
    return retrieveDatasetWithHttpInfo(id).getData();
      }

  /**
   * 
   * Retrieve a dataset by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;DatasetModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<DatasetModel> retrieveDatasetWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling retrieveDataset");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<DatasetModel> localVarReturnType = new GenericType<DatasetModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Retrieve the read and discover policies for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @return PolicyResponse
   * @throws ApiException if fails to make API call
   */
  public PolicyResponse retrieveDatasetPolicies(String id) throws ApiException {
    return retrieveDatasetPoliciesWithHttpInfo(id).getData();
      }

  /**
   * 
   * Retrieve the read and discover policies for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;PolicyResponse&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<PolicyResponse> retrieveDatasetPoliciesWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling retrieveDatasetPolicies");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/datasets/{id}/policies"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<PolicyResponse> localVarReturnType = new GenericType<PolicyResponse>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Retrieve a job&#39;s status by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return JobModel
   * @throws ApiException if fails to make API call
   */
  public JobModel retrieveJob(String id) throws ApiException {
    return retrieveJobWithHttpInfo(id).getData();
      }

  /**
   * 
   * Retrieve a job&#39;s status by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;JobModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<JobModel> retrieveJobWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling retrieveJob");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/jobs/{id}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<JobModel> localVarReturnType = new GenericType<JobModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Retrieve a job&#39;s result by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return Object
   * @throws ApiException if fails to make API call
   */
  public Object retrieveJobResult(String id) throws ApiException {
    return retrieveJobResultWithHttpInfo(id).getData();
      }

  /**
   * 
   * Retrieve a job&#39;s result by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;Object&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<Object> retrieveJobResultWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling retrieveJobResult");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/jobs/{id}/result"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<Object> localVarReturnType = new GenericType<Object>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Retrieve a snapshot by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return SnapshotModel
   * @throws ApiException if fails to make API call
   */
  public SnapshotModel retrieveSnapshot(String id) throws ApiException {
    return retrieveSnapshotWithHttpInfo(id).getData();
      }

  /**
   * 
   * Retrieve a snapshot by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;SnapshotModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<SnapshotModel> retrieveSnapshotWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling retrieveSnapshot");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots/{id}"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<SnapshotModel> localVarReturnType = new GenericType<SnapshotModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Retrieve the read and discover policies for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @return PolicyResponse
   * @throws ApiException if fails to make API call
   */
  public PolicyResponse retrieveSnapshotPolicies(String id) throws ApiException {
    return retrieveSnapshotPoliciesWithHttpInfo(id).getData();
      }

  /**
   * 
   * Retrieve the read and discover policies for the snapshot
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;PolicyResponse&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<PolicyResponse> retrieveSnapshotPoliciesWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling retrieveSnapshotPolicies");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/snapshots/{id}/policies"
      .replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<PolicyResponse> localVarReturnType = new GenericType<PolicyResponse>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Set the a group of configurations
   * @param configModel  (optional)
   * @return ConfigListModel
   * @throws ApiException if fails to make API call
   */
  public ConfigListModel setConfigList(ConfigGroupModel configModel) throws ApiException {
    return setConfigListWithHttpInfo(configModel).getData();
      }

  /**
   * 
   * Set the a group of configurations
   * @param configModel  (optional)
   * @return ApiResponse&lt;ConfigListModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<ConfigListModel> setConfigListWithHttpInfo(ConfigGroupModel configModel) throws ApiException {
    Object localVarPostBody = configModel;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/configs";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<ConfigListModel> localVarReturnType = new GenericType<ConfigListModel>() {};
    return apiClient.invokeAPI(localVarPath, "PUT", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Enable or disable the named fault. Performing the put on a config that is not a fault is an error. 
   * @param name name of the configuration to (required)
   * @param configEnable  (optional)
   * @throws ApiException if fails to make API call
   */
  public void setFault(String name, ConfigEnableModel configEnable) throws ApiException {

    setFaultWithHttpInfo(name, configEnable);
  }

  /**
   * 
   * Enable or disable the named fault. Performing the put on a config that is not a fault is an error. 
   * @param name name of the configuration to (required)
   * @param configEnable  (optional)
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<Void> setFaultWithHttpInfo(String name, ConfigEnableModel configEnable) throws ApiException {
    Object localVarPostBody = configEnable;
    
    // verify the required parameter 'name' is set
    if (name == null) {
      throw new ApiException(400, "Missing the required parameter 'name' when calling setFault");
    }
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/configs/{name}"
      .replaceAll("\\{" + "name" + "\\}", apiClient.escapeString(name.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };


    return apiClient.invokeAPI(localVarPath, "PUT", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, null);
  }
  /**
   * 
   * Returns whether the user is registered with terra 
   * @return UserStatusInfo
   * @throws ApiException if fails to make API call
   */
  public UserStatusInfo user() throws ApiException {
    return userWithHttpInfo().getData();
      }

  /**
   * 
   * Returns whether the user is registered with terra 
   * @return ApiResponse&lt;UserStatusInfo&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<UserStatusInfo> userWithHttpInfo() throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/repository/v1/register/user";

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();


    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<UserStatusInfo> localVarReturnType = new GenericType<UserStatusInfo>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
}
