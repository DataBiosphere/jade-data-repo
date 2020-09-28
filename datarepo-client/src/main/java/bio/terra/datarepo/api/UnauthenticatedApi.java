package bio.terra.datarepo.api;

import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiResponse;
import bio.terra.datarepo.client.Configuration;
import bio.terra.datarepo.client.Pair;

import javax.ws.rs.core.GenericType;

import bio.terra.datarepo.model.RepositoryConfigurationModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class UnauthenticatedApi {
  private ApiClient apiClient;

  public UnauthenticatedApi() {
    this(Configuration.getDefaultApiClient());
  }

  public UnauthenticatedApi(ApiClient apiClient) {
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
   * Retrieve the repository configuration information
   * @return RepositoryConfigurationModel
   * @throws ApiException if fails to make API call
   */
  public RepositoryConfigurationModel retrieveRepositoryConfig() throws ApiException {
    return retrieveRepositoryConfigWithHttpInfo().getData();
      }

  /**
   * 
   * Retrieve the repository configuration information
   * @return ApiResponse&lt;RepositoryConfigurationModel&gt;
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<RepositoryConfigurationModel> retrieveRepositoryConfigWithHttpInfo() throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/configuration";

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

    String[] localVarAuthNames = new String[] {  };

    GenericType<RepositoryConfigurationModel> localVarReturnType = new GenericType<RepositoryConfigurationModel>() {};
    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
      }
  /**
   * 
   * Returns the operational status of the service 
   * @throws ApiException if fails to make API call
   */
  public void serviceStatus() throws ApiException {

    serviceStatusWithHttpInfo();
  }

  /**
   * 
   * Returns the operational status of the service 
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<Void> serviceStatusWithHttpInfo() throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/status";

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

    String[] localVarAuthNames = new String[] {  };


    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, null);
  }
  /**
   * 
   * Requests that this instance of DR Manager shut down. In production, this must be configured to only be callable by Kubernetes. 
   * @throws ApiException if fails to make API call
   */
  public void shutdownRequest() throws ApiException {

    shutdownRequestWithHttpInfo();
  }

  /**
   * 
   * Requests that this instance of DR Manager shut down. In production, this must be configured to only be callable by Kubernetes. 
   * @throws ApiException if fails to make API call
   */
  public ApiResponse<Void> shutdownRequestWithHttpInfo() throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/shutdown";

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

    String[] localVarAuthNames = new String[] {  };


    return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, null);
  }
}
