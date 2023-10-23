package scripts.utils.tdrwrapper;

// This class wraps the TDR client, providing these features:
// 1. Raw ApiExceptions are mapped to specific DataRepoClient exceptions based on the http error
// status. Those exceptions have the ErrorModel deserialized.
// 2. Futures for waiting for and retrieving results of async calls.
// 3. Methods that automatically wait.

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;
import bio.terra.datarepo.model.ErrorModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;
import bio.terra.datarepo.model.SnapshotRequestModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpStatusCodes;
import common.utils.FileUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;
import runner.config.TestUserSpecification;
import scripts.utils.DataRepoUtils;
import scripts.utils.tdrwrapper.exception.DataRepoBadRequestClientException;
import scripts.utils.tdrwrapper.exception.DataRepoClientException;
import scripts.utils.tdrwrapper.exception.DataRepoConflictClientException;
import scripts.utils.tdrwrapper.exception.DataRepoForbiddenClientException;
import scripts.utils.tdrwrapper.exception.DataRepoInternalServiceClientException;
import scripts.utils.tdrwrapper.exception.DataRepoNotFoundClientException;
import scripts.utils.tdrwrapper.exception.DataRepoNotImplementedClientException;
import scripts.utils.tdrwrapper.exception.DataRepoServiceUnavailableClientException;
import scripts.utils.tdrwrapper.exception.DataRepoUnauthorizedClientException;
import scripts.utils.tdrwrapper.exception.DataRepoUnknownClientException;

public class DataRepoWrap {
  private static final Logger logger = LoggerFactory.getLogger(DataRepoWrap.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final RepositoryApi repositoryApi;
  private final ResourcesApi resourcesApi;

  public DataRepoWrap(RepositoryApi repositoryApi, ResourcesApi resourcesApi) {
    this.repositoryApi = repositoryApi;
    this.resourcesApi = resourcesApi;
  }

  public static DataRepoWrap wrapFactory(TestUserSpecification testUser, ServerSpecification server)
      throws IOException {

    ApiClient ownerUser1Client = DataRepoUtils.getClientForTestUser(testUser, server);
    return new DataRepoWrap(
        new RepositoryApi(ownerUser1Client), new ResourcesApi(ownerUser1Client));
  }

  /**
   * Function wrapper that converts openapi ApiException into DataRepoClient exceptions
   *
   * @param function a datarepo client call
   * @param <T> success return type
   * @return T returned on success
   * @throws DataRepoClientException usually a DataRepoClientException subclass
   */
  public static <T> T apiCallThrow(ApiFunction<T> function) {
    try {
      return function.apply();
    } catch (ApiException apiException) {
      throw fromApiException(apiException);
    }
  }

  private static DataRepoClientException fromApiException(ApiException apiException) {
    int statusCode = apiException.getCode();
    List<String> errorDetails = null;
    String message = StringUtils.EMPTY;
    // We don't expect to see this case, but don't want to barf if it happens
    if (!HttpStatusCodes.isSuccess(statusCode)) {
      String responseBody = apiException.getResponseBody();
      if (responseBody != null) {
        try {
          ErrorModel errorModel = objectMapper.readValue(responseBody, ErrorModel.class);
          errorDetails = errorModel.getErrorDetail();
          message = errorModel.getMessage();
        } catch (JsonProcessingException ex) {
          message = responseBody;
        }
      }
    }
    switch (statusCode) {
      case HttpStatusCodes.STATUS_CODE_BAD_REQUEST:
        return new DataRepoBadRequestClientException(
            message, statusCode, errorDetails, apiException);
      case HttpStatusCodes.STATUS_CODE_CONFLICT:
        return new DataRepoConflictClientException(message, statusCode, errorDetails, apiException);
      case HttpStatusCodes.STATUS_CODE_FORBIDDEN:
        return new DataRepoForbiddenClientException(
            message, statusCode, errorDetails, apiException);
      case HttpStatusCodes.STATUS_CODE_NOT_FOUND:
        return new DataRepoNotFoundClientException(message, statusCode, errorDetails, apiException);
      case HttpStatusCodes.STATUS_CODE_SERVER_ERROR:
        return new DataRepoInternalServiceClientException(
            message, statusCode, errorDetails, apiException);
      case HttpStatusCodes.STATUS_CODE_SERVICE_UNAVAILABLE:
        return new DataRepoServiceUnavailableClientException(
            "Service Unavailable", statusCode, errorDetails, apiException);
      case HttpStatusCodes.STATUS_CODE_UNAUTHORIZED:
        return new DataRepoUnauthorizedClientException(
            message, statusCode, errorDetails, apiException);
      case 501: // not implemented - no HttpStatusCodes for that
        return new DataRepoNotImplementedClientException(
            message, statusCode, errorDetails, apiException);
      default:
        return new DataRepoUnknownClientException(
            "Unknown Exception", statusCode, errorDetails, apiException);
    }
  }

  // -- billing profile alphabetically --

  public PolicyResponse addProfilePolicyMember(
      UUID profileId, String policyName, String userEmail) {
    PolicyMemberRequest addRequest = new PolicyMemberRequest().email(userEmail);
    return apiCallThrow(
        () -> resourcesApi.addProfilePolicyMember(addRequest, profileId, policyName));
  }

  public BillingProfileModel createProfile(
      String billingAccount, String profileName, boolean randomizeName) throws Exception {

    WrapFuture<BillingProfileModel> wrapFuture =
        createProfileFuture(billingAccount, profileName, randomizeName);

    return wrapFuture.get();
  }

  public WrapFuture<BillingProfileModel> createProfileFuture(
      String billingAccount, String profileName, boolean randomizeName) {

    if (randomizeName) {
      profileName = FileUtils.randomizeName(profileName);
    }

    BillingProfileRequestModel createProfileRequest =
        new BillingProfileRequestModel()
            .id(UUID.randomUUID())
            .biller("direct")
            .billingAccountId(billingAccount)
            .profileName(profileName)
            .description(profileName + " created in TestRunner RunTests");

    JobModel jobResponse = apiCallThrow(() -> resourcesApi.createProfile(createProfileRequest));

    return new WrapFuture<>(jobResponse.getId(), repositoryApi, BillingProfileModel.class);
  }

  public DeleteResponseModel deleteProfile(UUID profileId) throws Exception {
    WrapFuture<DeleteResponseModel> wrapFuture = deleteProfileFuture(profileId);
    return wrapFuture.get();
  }

  public WrapFuture<DeleteResponseModel> deleteProfileFuture(UUID profileId) {
    JobModel jobResponse = apiCallThrow(() -> resourcesApi.deleteProfile(profileId, false));
    return new WrapFuture<>(jobResponse.getId(), repositoryApi, DeleteResponseModel.class);
  }

  public PolicyResponse deleteProfilePolicyMember(
      UUID profileId, String policyName, String userEmail) {

    return apiCallThrow(
        () -> resourcesApi.deleteProfilePolicyMember(profileId, policyName, userEmail));
  }

  public EnumerateBillingProfileModel enumerateProfiles(Integer offset, Integer limit) {
    return DataRepoWrap.apiCallThrow(() -> resourcesApi.enumerateProfiles(offset, limit));
  }

  public BillingProfileModel retrieveProfile(UUID profileId) {
    return DataRepoWrap.apiCallThrow(() -> resourcesApi.retrieveProfile(profileId));
  }

  public PolicyResponse retrieveProfilePolicies(UUID id) {
    return apiCallThrow(() -> resourcesApi.retrieveProfilePolicies(id));
  }

  // -- dataset --

  public PolicyResponse addDatasetPolicyMember(UUID id, String policyName, String userEmail) {
    PolicyMemberRequest addRequest = new PolicyMemberRequest().email(userEmail);
    return apiCallThrow(() -> repositoryApi.addDatasetPolicyMember(id, policyName, addRequest));
  }

  public DatasetSummaryModel createDataset(
      UUID profileId, String apipayloadFilename, boolean randomizeName) throws Exception {

    WrapFuture<DatasetSummaryModel> wrapFuture =
        createDatasetFuture(profileId, apipayloadFilename, randomizeName);

    return wrapFuture.get();
  }

  public WrapFuture<DatasetSummaryModel> createDatasetFuture(
      UUID profileId, String apipayloadFilename, boolean randomizeName) throws Exception {
    // use Jackson to map the stream contents to a DatasetRequestModel object
    InputStream datasetRequestFile =
        FileUtils.getResourceFileHandle("apipayloads/" + apipayloadFilename);
    DatasetRequestModel createDatasetRequest =
        objectMapper.readValue(datasetRequestFile, DatasetRequestModel.class);
    createDatasetRequest.defaultProfileId(profileId);

    if (randomizeName) {
      createDatasetRequest.setName(FileUtils.randomizeName(createDatasetRequest.getName()));
    }

    JobModel jobResponse = apiCallThrow(() -> repositoryApi.createDataset(createDatasetRequest));

    return new WrapFuture<>(jobResponse.getId(), repositoryApi, DatasetSummaryModel.class);
  }

  public DeleteResponseModel deleteDataset(UUID id) throws Exception {
    WrapFuture<DeleteResponseModel> wrapFuture = deleteDatasetFuture(id);
    return wrapFuture.get();
  }

  public WrapFuture<DeleteResponseModel> deleteDatasetFuture(UUID id) throws Exception {
    JobModel jobResponse = apiCallThrow(() -> repositoryApi.deleteDataset(id));

    return new WrapFuture<>(jobResponse.getId(), repositoryApi, DeleteResponseModel.class);
  }

  // -- snapshot --

  public SnapshotSummaryModel createSnapshot(
      UUID profileId, String apipayloadFilename, String datasetName, boolean randomizeName)
      throws Exception {

    WrapFuture<SnapshotSummaryModel> wrapFuture =
        createSnapshotFuture(profileId, apipayloadFilename, datasetName, randomizeName);

    return wrapFuture.get();
  }

  public WrapFuture<SnapshotSummaryModel> createSnapshotFuture(
      UUID profileId, String apipayloadFilename, String datasetName, boolean randomizeName)
      throws Exception {
    // use Jackson to map the stream contents to a SnapshotRequestModel object
    InputStream SnapshotRequestFile =
        FileUtils.getResourceFileHandle("apipayloads/" + apipayloadFilename);
    SnapshotRequestModel createSnapshotRequest =
        objectMapper.readValue(SnapshotRequestFile, SnapshotRequestModel.class);
    createSnapshotRequest.profileId(profileId);
    createSnapshotRequest.getContents().get(0).setDatasetName(datasetName);

    if (randomizeName) {
      createSnapshotRequest.setName(FileUtils.randomizeName(createSnapshotRequest.getName()));
    }

    JobModel jobResponse = apiCallThrow(() -> repositoryApi.createSnapshot(createSnapshotRequest));

    return new WrapFuture<>(jobResponse.getId(), repositoryApi, SnapshotSummaryModel.class);
  }

  public DeleteResponseModel deleteSnapshot(UUID id) throws Exception {
    WrapFuture<DeleteResponseModel> wrapFuture = deleteSnapshotFuture(id);
    return wrapFuture.get();
  }

  public WrapFuture<DeleteResponseModel> deleteSnapshotFuture(UUID id) throws Exception {
    JobModel jobResponse = apiCallThrow(() -> repositoryApi.deleteSnapshot(id));

    return new WrapFuture<>(jobResponse.getId(), repositoryApi, DeleteResponseModel.class);
  }
}
