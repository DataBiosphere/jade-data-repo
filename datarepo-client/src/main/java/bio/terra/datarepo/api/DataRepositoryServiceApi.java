package bio.terra.datarepo.api;

import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiResponse;
import bio.terra.datarepo.client.Configuration;
import bio.terra.datarepo.client.Pair;

import javax.ws.rs.core.GenericType;

import bio.terra.datarepo.model.DRSAccessURL;
import bio.terra.datarepo.model.DRSError;
import bio.terra.datarepo.model.DRSObject;
import bio.terra.datarepo.model.DRSServiceInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class DataRepositoryServiceApi {
  private ApiClient apiClient;

  public DataRepositoryServiceApi() {
    this(Configuration.getDefaultApiClient());
  }

  public DataRepositoryServiceApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  /**
   * Get a URL for fetching bytes.
   * Returns a URL that can be used to fetch the object bytes. This method only needs to be called when using an &#x60;AccessMethod&#x60; that contains an &#x60;access_id&#x60; (e.g., for servers that use signed URLs for fetching object bytes).
   * @param objectId An &#x60;id&#x60; of a Data Object (required)
   * @param accessId An &#x60;access_id&#x60; from the &#x60;access_methods&#x60; list of a Data Object (required)
   * @return DRSAccessURL
   * @throws ApiException if fails to make API call
   */
  public DRSAccessURL getAccessURL(String objectId, String accessId) throws ApiException {
    return getAccessURLWithHttpInfo(objectId, accessId).getData();
      }

  /**
   * Get a URL for fetching bytes.
   * Returns a URL that can be used to fetch the object bytes. This method only needs to be called when using an &#x60;AccessMethod&#x60; that contains an &#x60;access_id&#x60; (e.g., for servers that use signed URLs for fetching object bytes).
   * @param objectId An &#x60;id&#x60; of a Data Object (required)
   * @param accessId An &#x60;access_id&#x60; from the &#x60;access_methods&#x60; list of a Data Object (required)
   * @return ApiResponse&lt;DRSAccessURL&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<DRSAccessURL> getAccessURLWithHttpInfo(String objectId, String accessId) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'objectId' is set
    if (objectId == null) {
      throw new ApiException(400, "Missing the required parameter 'objectId' when calling getAccessURL");
    }
    
    // verify the required parameter 'accessId' is set
    if (accessId == null) {
      throw new ApiException(400, "Missing the required parameter 'accessId' when calling getAccessURL");
    }
    
    // create path and map variables
    String localVarPath = "/ga4gh/drs/v1/objects/{object_id}/access/{access_id}"
      .replaceAll("\\{" + "object_id" + "\\}", apiClient.escapeString(objectId.toString()))
      .replaceAll("\\{" + "access_id" + "\\}", apiClient.escapeString(accessId.toString()));

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

    GenericType<DRSAccessURL> localVarReturnType = new GenericType<DRSAccessURL>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * Get info about an &#x60;Object&#x60;.
   * Returns object metadata, and a list of access methods that can be used to fetch object bytes.
   * @param objectId  (required)
   * @param expand If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains another bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. (optional, default to false)
   * @return DRSObject
   * @throws ApiException if fails to make API call
   */
  public DRSObject getObject(String objectId, Boolean expand) throws ApiException {
    return getObjectWithHttpInfo(objectId, expand).getData();
      }

  /**
   * Get info about an &#x60;Object&#x60;.
   * Returns object metadata, and a list of access methods that can be used to fetch object bytes.
   * @param objectId  (required)
   * @param expand If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains another bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. (optional, default to false)
   * @return ApiResponse&lt;DRSObject&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<DRSObject> getObjectWithHttpInfo(String objectId, Boolean expand) throws ApiException {
    Object localVarPostBody = null;
    
    // verify the required parameter 'objectId' is set
    if (objectId == null) {
      throw new ApiException(400, "Missing the required parameter 'objectId' when calling getObject");
    }
    
    // create path and map variables
    String localVarPath = "/ga4gh/drs/v1/objects/{object_id}"
      .replaceAll("\\{" + "object_id" + "\\}", apiClient.escapeString(objectId.toString()));

    // query params
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPairs("", "expand", expand));

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "googleoauth" };

    GenericType<DRSObject> localVarReturnType = new GenericType<DRSObject>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * Get information about this implementation.
   * May return service version and other information. [dd]NOTE: technically, this has been removed from DRS V1.0. It will be added back when there is a common service_info across ga4gh. I don&#39;t expect it to be too different, so just leaving this info call in place.
   * @return DRSServiceInfo
   * @throws ApiException if fails to make API call
   */
  public DRSServiceInfo getServiceInfo() throws ApiException {
    return getServiceInfoWithHttpInfo().getData();
      }

  /**
   * Get information about this implementation.
   * May return service version and other information. [dd]NOTE: technically, this has been removed from DRS V1.0. It will be added back when there is a common service_info across ga4gh. I don&#39;t expect it to be too different, so just leaving this info call in place.
   * @return ApiResponse&lt;DRSServiceInfo&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<DRSServiceInfo> getServiceInfoWithHttpInfo() throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/ga4gh/drs/v1/service-info";

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

    GenericType<DRSServiceInfo> localVarReturnType = new GenericType<DRSServiceInfo>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
}
