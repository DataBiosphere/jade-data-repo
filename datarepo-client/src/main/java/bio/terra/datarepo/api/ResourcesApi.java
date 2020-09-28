package bio.terra.datarepo.api;

import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiResponse;
import bio.terra.datarepo.client.Configuration;
import bio.terra.datarepo.client.Pair;

import javax.ws.rs.core.GenericType;

import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;
import bio.terra.datarepo.model.ErrorModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class ResourcesApi {
  private ApiClient apiClient;

  public ResourcesApi() {
    this(Configuration.getDefaultApiClient());
  }

  public ResourcesApi(ApiClient apiClient) {
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
   * Creates a new profile associated with a billing account 
   * @param billingProfileRequest  (optional)
   * @return BillingProfileModel
   * @throws ApiException if fails to make API call
   */
  public BillingProfileModel createProfile(BillingProfileRequestModel billingProfileRequest) throws ApiException {
    return createProfileWithHttpInfo(billingProfileRequest).getData();
      }

  /**
   * 
   * Creates a new profile associated with a billing account 
   * @param billingProfileRequest  (optional)
   * @return ApiResponse&lt;BillingProfileModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<BillingProfileModel> createProfileWithHttpInfo(BillingProfileRequestModel billingProfileRequest) throws ApiException {
    Object localVarPostBody = billingProfileRequest;
    
    // create path and map variables
    String localVarPath = "/api/resources/v1/profiles";

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

    GenericType<BillingProfileModel> localVarReturnType = new GenericType<BillingProfileModel>() {};
    return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Delete a billing profile by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return DeleteResponseModel
   * @throws ApiException if fails to make API call
   */
  public DeleteResponseModel deleteProfile(String id) throws ApiException {
    return deleteProfileWithHttpInfo(id).getData();
      }

  /**
   * 
   * Delete a billing profile by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;DeleteResponseModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<DeleteResponseModel> deleteProfileWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling deleteProfile");
    }
    
    // create path and map variables
    String localVarPath = "/api/resources/v1/profiles/{id}"
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

    GenericType<DeleteResponseModel> localVarReturnType = new GenericType<DeleteResponseModel>() {};
    return apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Returns a list of all of the billing profiles 
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @return EnumerateBillingProfileModel
   * @throws ApiException if fails to make API call
   */
  public EnumerateBillingProfileModel enumerateProfiles(Integer offset, Integer limit) throws ApiException {
    return enumerateProfilesWithHttpInfo(offset, limit).getData();
      }

  /**
   * 
   * Returns a list of all of the billing profiles 
   * @param offset The number of items to skip before starting to collect the result set. (optional, default to 0)
   * @param limit The numbers of items to return. (optional, default to 10)
   * @return ApiResponse&lt;EnumerateBillingProfileModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<EnumerateBillingProfileModel> enumerateProfilesWithHttpInfo(Integer offset, Integer limit) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/resources/v1/profiles";

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

    GenericType<EnumerateBillingProfileModel> localVarReturnType = new GenericType<EnumerateBillingProfileModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Retrieve a billing profile by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return BillingProfileModel
   * @throws ApiException if fails to make API call
   */
  public BillingProfileModel retrieveProfile(String id) throws ApiException {
    return retrieveProfileWithHttpInfo(id).getData();
      }

  /**
   * 
   * Retrieve a billing profile by id
   * @param id A UUID to used to identify an object in the repository (required)
   * @return ApiResponse&lt;BillingProfileModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<BillingProfileModel> retrieveProfileWithHttpInfo(String id) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'id' is set
    if (id == null) {
      throw new ApiException(400, "Missing the required parameter 'id' when calling retrieveProfile");
    }
    
    // create path and map variables
    String localVarPath = "/api/resources/v1/profiles/{id}"
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

    GenericType<BillingProfileModel> localVarReturnType = new GenericType<BillingProfileModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
}
