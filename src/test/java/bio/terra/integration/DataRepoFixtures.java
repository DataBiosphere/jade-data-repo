package bio.terra.integration;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.TestUtils;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.AssetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.ConfigEnableModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigListModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetRequestModelPolicies;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyResponse;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.TransactionCloseModel;
import bio.terra.model.TransactionCreateModel;
import bio.terra.model.TransactionModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.filedata.DrsResponse;
import bio.terra.service.filedata.DrsService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
public class DataRepoFixtures {

  private static final Logger logger = LoggerFactory.getLogger(DataRepoFixtures.class);

  private static final String QUERY_TEMPLATE =
      "resource.type=\"k8s_container\"\n"
          + "resource.labels.project_id=\"broad-jade-integration\"\n"
          + "resource.labels.location=\"us-central1\"\n"
          + "resource.labels.cluster_name=\"integration-master\"\n"
          + "resource.labels.namespace_name=\"integration-<intNumber>\"\n"
          + "labels.k8s-pod/component=\"integration-<intNumber>-jade-datarepo-api\"\n"
          + "<if(hasFlightId)>jsonPayload.flightId=\"<flightId>\"<endif>";

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private TestConfiguration testConfig;

  // Create a Billing Profile model: expect successful creation
  public BillingProfileModel createBillingProfile(TestConfiguration.User user) throws Exception {
    BillingProfileRequestModel billingProfileRequestModel =
        ProfileFixtures.billingProfileRequest(
            ProfileFixtures.billingProfileForAccount(testConfig.getGoogleBillingAccountId()));
    String json = TestUtils.mapToJson(billingProfileRequestModel);

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.post(user, "/api/resources/v1/profiles", json, new TypeReference<>() {});
    assertTrue("profile create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "profile create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<BillingProfileModel> postResponse =
        dataRepoClient.waitForResponse(user, jobResponse, new TypeReference<>() {});

    BillingProfileModel billingProfile =
        validateResponse(postResponse, "billing profile create", HttpStatus.CREATED, jobResponse);
    logger.info("Billing profile created: {}", billingProfile);
    return billingProfile;
  }

  // Create a Billing Profile model: expect successful creation
  public BillingProfileModel createAzureBillingProfile(TestConfiguration.User user)
      throws Exception {
    BillingProfileRequestModel billingProfileRequestModel =
        ProfileFixtures.billingProfileRequest(
            ProfileFixtures.billingProfileForDeployedApplication(
                testConfig.getTargetTenantId(),
                testConfig.getTargetSubscriptionId(),
                testConfig.getTargetResourceGroupName(),
                testConfig.getTargetApplicationName()));
    String json = TestUtils.mapToJson(billingProfileRequestModel);

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.post(user, "/api/resources/v1/profiles", json, new TypeReference<>() {});
    assertTrue("profile create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "profile create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<BillingProfileModel> postResponse =
        dataRepoClient.waitForResponse(user, jobResponse, new TypeReference<>() {});

    return validateResponse(
        postResponse, "azure billing profile create", HttpStatus.CREATED, jobResponse);
  }

  public void deleteProfile(TestConfiguration.User user, UUID profileId) throws Exception {
    DataRepoResponse<DeleteResponseModel> deleteResponse = deleteProfileLog(user, profileId);
    assertGoodDeleteResponse(deleteResponse);
  }

  public DataRepoResponse<DeleteResponseModel> deleteProfileLog(
      TestConfiguration.User user, UUID profileId) throws Exception {

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.delete(
            user, "/api/resources/v1/profiles/" + profileId, new TypeReference<>() {});
    assertTrue("profile delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "profile delete launch response is present", jobResponse.getResponseObject().isPresent());

    return dataRepoClient.waitForResponseLog(user, jobResponse, new TypeReference<>() {});
  }

  // datasets

  public DataRepoResponse<JobModel> createDatasetRaw(
      TestConfiguration.User user,
      UUID profileId,
      String filename,
      CloudPlatform cloudPlatform,
      boolean usePetAccount,
      boolean selfHosted,
      boolean dedicatedServiceAccount,
      boolean predictableIds,
      DatasetRequestModelPolicies policies)
      throws Exception {
    DatasetRequestModel requestModel = jsonLoader.loadObject(filename, DatasetRequestModel.class);
    requestModel.setDefaultProfileId(profileId);
    requestModel.setName(Names.randomizeName(requestModel.getName()));
    if (cloudPlatform != null && requestModel.getCloudPlatform() == null) {
      requestModel.setCloudPlatform(cloudPlatform);
    }
    requestModel.experimentalSelfHosted(selfHosted);
    requestModel.experimentalPredictableFileIds(predictableIds);
    requestModel.dedicatedIngestServiceAccount(dedicatedServiceAccount);
    requestModel.policies(policies);
    String json = TestUtils.mapToJson(requestModel);

    return dataRepoClient.post(
        user, "/api/repository/v1/datasets", json, new TypeReference<>() {}, usePetAccount);
  }

  public DatasetSummaryModel createDataset(
      TestConfiguration.User user, UUID profileId, String filename) throws Exception {
    return createDataset(user, profileId, filename, CloudPlatformWrapper.DEFAULT);
  }

  public DatasetSummaryModel createDataset(
      TestConfiguration.User user, UUID profileId, String filename, CloudPlatform cloudPlatform)
      throws Exception {
    return createDataset(user, profileId, filename, cloudPlatform, false);
  }

  public DatasetSummaryModel createDataset(
      TestConfiguration.User user, DatasetRequestModel requestModel, boolean usePetAccount)
      throws Exception {
    String json = TestUtils.mapToJson(requestModel);
    DataRepoResponse<JobModel> post =
        dataRepoClient.post(
            user, "/api/repository/v1/datasets", json, new TypeReference<>() {}, usePetAccount);
    return waitForDatasetCreate(user, post);
  }

  public DatasetSummaryModel createSelfHostedDataset(
      TestConfiguration.User user, UUID profileId, String fileName, boolean dedicatedServiceAccount)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createDatasetRaw(
            user,
            profileId,
            fileName,
            CloudPlatform.GCP,
            false,
            true,
            dedicatedServiceAccount,
            false,
            null);
    return waitForDatasetCreate(user, jobResponse);
  }

  public DatasetSummaryModel createDatasetWithOwnServiceAccount(
      TestConfiguration.User user, UUID profileId, String fileName) throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createDatasetRaw(
            user, profileId, fileName, CloudPlatform.GCP, false, false, true, false, null);
    return waitForDatasetCreate(user, jobResponse);
  }

  public DatasetSummaryModel createDataset(
      TestConfiguration.User user,
      UUID profileId,
      String filename,
      CloudPlatform cloudPlatform,
      boolean usePetAccount)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createDatasetRaw(
            user, profileId, filename, cloudPlatform, usePetAccount, false, false, false, null);
    return waitForDatasetCreate(user, jobResponse);
  }

  public DatasetSummaryModel createDatasetWithPolicies(
      TestConfiguration.User user,
      UUID profileId,
      String filename,
      DatasetRequestModelPolicies policies)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createDatasetRaw(
            user, profileId, filename, CloudPlatform.GCP, false, false, false, false, policies);
    return waitForDatasetCreate(user, jobResponse);
  }

  public DatasetSummaryModel waitForDatasetCreate(
      TestConfiguration.User user, DataRepoResponse<JobModel> jobResponse) throws Exception {
    assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "dataset create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<DatasetSummaryModel> response =
        dataRepoClient.waitForResponseLog(user, jobResponse, new TypeReference<>() {});
    DatasetSummaryModel datasetSummary =
        validateResponse(response, "dataset create", HttpStatus.CREATED, jobResponse);
    logger.info("Dataset created: {}", datasetSummary);
    return datasetSummary;
  }

  public void createDatasetError(
      TestConfiguration.User user, UUID profileId, String filename, HttpStatus checkStatus)
      throws Exception {
    createDatasetError(user, profileId, filename, checkStatus, CloudPlatform.GCP);
  }

  public void createDatasetError(
      TestConfiguration.User user,
      UUID profileId,
      String filename,
      HttpStatus checkStatus,
      CloudPlatform cloudPlatform)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createDatasetRaw(
            user, profileId, filename, cloudPlatform, false, false, false, false, null);
    assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "dataset create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<ErrorModel> response =
        dataRepoClient.waitForResponse(user, jobResponse, new TypeReference<>() {});
    if (checkStatus == null) {
      assertFalse("dataset create is failure", response.getStatusCode().is2xxSuccessful());
    } else {
      assertThat("correct dataset create error", response.getStatusCode(), equalTo(checkStatus));
    }
    assertTrue("dataset create error response is present", response.getErrorObject().isPresent());
  }

  public DataRepoResponse<JobModel> deleteDataRaw(
      TestConfiguration.User user, UUID datasetId, DataDeletionRequest request) throws Exception {
    String url = String.format("/api/repository/v1/datasets/%s/deletes", datasetId);
    String json = TestUtils.mapToJson(request);
    return dataRepoClient.post(user, url, json, new TypeReference<>() {});
  }

  public void deleteData(TestConfiguration.User user, UUID datasetId, DataDeletionRequest request)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse = deleteDataRaw(user, datasetId, request);
    DataRepoResponse<DeleteResponseModel> deleteResponse =
        dataRepoClient.waitForResponse(user, jobResponse, new TypeReference<>() {});
    assertGoodDeleteResponse(deleteResponse);
  }

  public DataRepoResponse<JobModel> deleteDatasetLaunch(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    return dataRepoClient.delete(
        user, "/api/repository/v1/datasets/" + datasetId, new TypeReference<>() {});
  }

  public void deleteDataset(TestConfiguration.User user, UUID datasetId) throws Exception {
    DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDatasetLog(user, datasetId);
    assertGoodDeleteResponse(deleteResponse);
  }

  public void deleteDatasetShouldFail(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDatasetLog(user, datasetId);
    assertThat(
        "delete is not successful",
        deleteResponse.getStatusCode(),
        is(not(equalTo(HttpStatus.OK))));
  }

  public DataRepoResponse<DeleteResponseModel> deleteDatasetLog(
      TestConfiguration.User user, UUID datasetId) throws Exception {

    DataRepoResponse<JobModel> jobResponse = deleteDatasetLaunch(user, datasetId);
    assertTrue("dataset delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "dataset delete launch response is present", jobResponse.getResponseObject().isPresent());

    return dataRepoClient.waitForResponseLog(user, jobResponse, new TypeReference<>() {});
  }

  public DataRepoResponse<EnumerateDatasetModel> enumerateDatasetsRaw(TestConfiguration.User user)
      throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/datasets?sort=created_date&direction=desc",
        new TypeReference<>() {});
  }

  public EnumerateDatasetModel enumerateDatasets(TestConfiguration.User user) throws Exception {
    DataRepoResponse<EnumerateDatasetModel> response = enumerateDatasetsRaw(user);
    assertThat(
        "dataset enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("dataset get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<DatasetModel> getDatasetRaw(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    return getDatasetRaw(user, datasetId, null);
  }

  public DataRepoResponse<DatasetModel> getDatasetRaw(
      TestConfiguration.User user, UUID datasetId, List<DatasetRequestAccessIncludeModel> include)
      throws Exception {
    String includeQuery;
    if (include == null || include.isEmpty()) {
      includeQuery = "";
    } else {
      includeQuery =
          "?" + include.stream().map(i -> "include=" + i).collect(Collectors.joining("&"));
    }
    return dataRepoClient.get(
        user, "/api/repository/v1/datasets/" + datasetId + includeQuery, new TypeReference<>() {});
  }

  public DatasetModel getDataset(TestConfiguration.User user, UUID datasetId) throws Exception {
    return getDataset(user, datasetId, null);
  }

  public DatasetModel getDataset(
      TestConfiguration.User user, UUID datasetId, List<DatasetRequestAccessIncludeModel> include)
      throws Exception {
    DataRepoResponse<DatasetModel> response = getDatasetRaw(user, datasetId, include);
    return validateResponse(response, "dataset retrieve", HttpStatus.OK, null);
  }

  public DataRepoResponse<Object> addPolicyMemberRaw(
      TestConfiguration.User user,
      UUID resourceId,
      IamRole role,
      String userEmail,
      IamResourceType iamResourceType)
      throws Exception {
    PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
    String pathPrefix =
        switch (iamResourceType) {
          case DATASET -> "/api/repository/v1/datasets/";
          case DATASNAPSHOT -> "/api/repository/v1/snapshots/";
          case SPEND_PROFILE -> "/api/resources/v1/profiles/";
          default -> throw new IllegalArgumentException(
              "Policy member addition undefined for IamResourceType " + iamResourceType);
        };
    String path = pathPrefix + resourceId + "/policies/" + role.toString() + "/members";
    return dataRepoClient.post(user, path, TestUtils.mapToJson(req), new TypeReference<>() {});
  }

  public void addPolicyMember(
      TestConfiguration.User user,
      UUID resourceId,
      IamRole role,
      String newMemberEmail,
      IamResourceType iamResourceType)
      throws Exception {
    DataRepoResponse<Object> response =
        addPolicyMemberRaw(user, resourceId, role, newMemberEmail, iamResourceType);
    assertThat(
        iamResourceType + " policy member is successfully added",
        response.getStatusCode(),
        equalTo(HttpStatus.OK));
  }

  // adding dataset policy
  public void addDatasetPolicyMember(
      TestConfiguration.User user, UUID datasetId, IamRole role, String newMemberEmail)
      throws Exception {
    addPolicyMember(user, datasetId, role, newMemberEmail, IamResourceType.DATASET);
  }

  // getting a user's roles on a resource
  public DataRepoResponse<List<String>> retrieveUserRolesRaw(
      TestConfiguration.User user, UUID resourceId, IamResourceType iamResourceType)
      throws Exception {
    String pathPrefix =
        switch (iamResourceType) {
          case DATASET -> "/api/repository/v1/datasets/";
          case DATASNAPSHOT -> "/api/repository/v1/snapshots/";
          default -> throw new IllegalArgumentException(
              "Role fetch undefined for IamResourceType " + iamResourceType);
        };
    String path = pathPrefix + resourceId + "/roles";

    return dataRepoClient.get(user, path, new TypeReference<>() {});
  }

  public List<String> retrieveUserDatasetRoles(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    DataRepoResponse<List<String>> response =
        retrieveUserRolesRaw(user, datasetId, IamResourceType.DATASET);
    return validateResponse(response, "retrieving dataset roles", HttpStatus.OK, null);
  }

  // getting a resource's policies
  public DataRepoResponse<PolicyResponse> retrievePoliciesRaw(
      TestConfiguration.User user, UUID resourceId, IamResourceType iamResourceType)
      throws Exception {
    String pathPrefix =
        switch (iamResourceType) {
          case DATASET -> "/api/repository/v1/datasets/";
          case DATASNAPSHOT -> "/api/repository/v1/snapshots/";
          case SPEND_PROFILE -> "/api/resources/v1/profiles/";
          default -> throw new IllegalArgumentException(
              "Policy fetch undefined for IamResourceType " + iamResourceType);
        };
    String path = pathPrefix + resourceId + "/policies";

    return dataRepoClient.get(user, path, new TypeReference<>() {});
  }

  public PolicyResponse retrieveDatasetPolicies(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    DataRepoResponse<PolicyResponse> response =
        retrievePoliciesRaw(user, datasetId, IamResourceType.DATASET);
    return validateResponse(response, "retrieving dataset policies", HttpStatus.OK, null);
  }

  public PolicyResponse retrieveSnapshotPolicies(TestConfiguration.User user, UUID snapshotId)
      throws Exception {
    DataRepoResponse<PolicyResponse> response =
        retrievePoliciesRaw(user, snapshotId, IamResourceType.DATASNAPSHOT);
    return validateResponse(response, "retrieving snapshot policies", HttpStatus.OK, null);
  }

  // adding dataset asset
  public DataRepoResponse<JobModel> addDatasetAssetRaw(
      TestConfiguration.User user, UUID datasetId, AssetModel assetModel) throws Exception {
    return dataRepoClient.post(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/assets",
        TestUtils.mapToJson(assetModel),
        new TypeReference<>() {});
  }

  public void addDatasetAsset(TestConfiguration.User user, UUID datasetId, AssetModel assetModel)
      throws Exception {
    // TODO add the assetModel as a builder object
    DataRepoResponse<JobModel> response = addDatasetAssetRaw(user, datasetId, assetModel);
    assertTrue(
        assetModel + " asset specification is successfully added",
        response.getStatusCode().is2xxSuccessful());
  }

  public ErrorModel addDatasetAssetExpectFailure(
      TestConfiguration.User user, UUID datasetId, AssetModel assetModel) throws Exception {
    DataRepoResponse<JobModel> response = addDatasetAssetRaw(user, datasetId, assetModel);
    assertTrue(
        assetModel + " job is successfully kicked off", response.getStatusCode().is2xxSuccessful());
    DataRepoResponse<ErrorModel> errorModel =
        dataRepoClient.waitForResponse(user, response, new TypeReference<>() {});
    assertTrue("dataset asset error response is present", errorModel.getErrorObject().isPresent());
    return errorModel.getErrorObject().get();
  }

  public DataRepoResponse<JobModel> deleteDatasetAssetRaw(
      TestConfiguration.User user, UUID datasetId, String assetName) throws Exception {
    return dataRepoClient.delete(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/assets/" + assetName,
        new TypeReference<>() {});
  }

  public void deleteDatasetAsset(TestConfiguration.User user, UUID datasetId, String assetName)
      throws Exception {
    DataRepoResponse<JobModel> response = deleteDatasetAssetRaw(user, datasetId, assetName);
    assertTrue(
        assetName + " asset specification is successfully deleted",
        response.getStatusCode().is2xxSuccessful());
  }

  public ErrorModel deleteDatasetAssetExpectFailure(
      TestConfiguration.User user, UUID datasetId, String assetName) throws Exception {
    DataRepoResponse<JobModel> response = deleteDatasetAssetRaw(user, datasetId, assetName);
    assertTrue(
        assetName + " delete job is successfully kicked off",
        response.getStatusCode().is2xxSuccessful());
    DataRepoResponse<ErrorModel> errorModel =
        dataRepoClient.waitForResponse(user, response, new TypeReference<>() {});
    assertTrue("dataset asset error response is present", errorModel.getErrorObject().isPresent());
    return errorModel.getErrorObject().get();
  }

  // snapshots

  // adding snapshot policy
  public void addSnapshotPolicyMember(
      TestConfiguration.User user, UUID snapshotId, IamRole role, String newMemberEmail)
      throws Exception {
    addPolicyMember(user, snapshotId, role, newMemberEmail, IamResourceType.DATASNAPSHOT);
  }

  public List<String> retrieveUserSnapshotRoles(TestConfiguration.User user, UUID datasetId)
      throws Exception {
    DataRepoResponse<List<String>> response =
        retrieveUserRolesRaw(user, datasetId, IamResourceType.DATASNAPSHOT);
    return validateResponse(response, "retrieving snapshot roles", HttpStatus.OK, null);
  }

  public DataRepoResponse<JobModel> createSnapshotRaw(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      SnapshotRequestModel requestModel,
      boolean randomizeName,
      boolean usePetAccount)
      throws Exception {

    if (randomizeName) {
      requestModel.setName(Names.randomizeName(requestModel.getName()));
    }
    requestModel.getContents().get(0).setDatasetName(datasetName);
    requestModel.setProfileId(profileId);
    String json = TestUtils.mapToJson(requestModel);

    return dataRepoClient.post(
        user, "/api/repository/v1/snapshots", json, new TypeReference<>() {}, usePetAccount);
  }

  public SnapshotSummaryModel createSnapshotWithRequest(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      SnapshotRequestModel snapshotRequest)
      throws Exception {
    return createSnapshotWithRequest(user, datasetName, profileId, snapshotRequest, true);
  }

  public SnapshotSummaryModel createSnapshotWithRequest(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      SnapshotRequestModel snapshotRequest,
      boolean randomizeName)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createSnapshotRaw(user, datasetName, profileId, snapshotRequest, randomizeName, false);
    return finishCreateSnapshot(user, jobResponse);
  }

  public SnapshotSummaryModel createSnapshotWithRequest(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      SnapshotRequestModel snapshotRequest,
      boolean randomizeName,
      boolean usePetAccount)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        createSnapshotRaw(
            user, datasetName, profileId, snapshotRequest, randomizeName, usePetAccount);
    return finishCreateSnapshot(user, jobResponse);
  }

  public SnapshotSummaryModel createSnapshot(
      TestConfiguration.User user, String datasetName, UUID profileId, String filename)
      throws Exception {
    return createSnapshot(user, datasetName, profileId, filename, true);
  }

  public SnapshotSummaryModel createSnapshot(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      String filename,
      boolean randomizeName)
      throws Exception {
    return createSnapshot(user, datasetName, profileId, filename, randomizeName, false);
  }

  public SnapshotSummaryModel createSnapshot(
      TestConfiguration.User user,
      String datasetName,
      UUID profileId,
      String filename,
      boolean randomizeName,
      boolean usePetAccount)
      throws Exception {
    SnapshotRequestModel requestModel = jsonLoader.loadObject(filename, SnapshotRequestModel.class);
    DataRepoResponse<JobModel> jobResponse =
        createSnapshotRaw(user, datasetName, profileId, requestModel, randomizeName, usePetAccount);
    return finishCreateSnapshot(user, jobResponse);
  }

  private SnapshotSummaryModel finishCreateSnapshot(
      TestConfiguration.User user, DataRepoResponse<JobModel> jobResponse) throws Exception {
    assertTrue("snapshot create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "snapshot create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<SnapshotSummaryModel> snapshotResponse =
        dataRepoClient.waitForResponse(user, jobResponse, new TypeReference<>() {});
    SnapshotSummaryModel snapshotSummary =
        validateResponse(snapshotResponse, "snapshot create", HttpStatus.CREATED, jobResponse);
    logger.info("Snapshot created: {}", snapshotSummary);
    return snapshotSummary;
  }

  public DataRepoResponse<SnapshotModel> getSnapshotRaw(
      TestConfiguration.User user, UUID snapshotId, List<SnapshotRetrieveIncludeModel> include)
      throws Exception {
    String includeParam;
    if (include != null && !include.isEmpty()) {
      includeParam =
          "?"
              + include.stream()
                  .map(i -> "include=" + i.toString())
                  .collect(Collectors.joining("&"));
    } else {
      includeParam = "";
    }
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots/" + snapshotId + includeParam,
        new TypeReference<>() {});
  }

  public SnapshotModel getSnapshot(
      TestConfiguration.User user, UUID snapshotId, List<SnapshotRetrieveIncludeModel> include)
      throws Exception {
    DataRepoResponse<SnapshotModel> response = getSnapshotRaw(user, snapshotId, include);
    return validateResponse(response, "snapshot retrieve", HttpStatus.OK, null);
  }

  /**
   * Given a dataset, table, and column, retrieve the DRS URI from the snapshot preview and extract
   * its DRS object ID.
   *
   * <p>In the past, we've queried BigQuery directly for this, but unpredictable delays in IAM
   * propagation make going through the preview more reliable (TDR proxies its request to BigQuery
   * here).
   */
  public String retrieveDrsIdFromSnapshotPreview(
      TestConfiguration.User user, UUID snapshotId, String table, String column) throws Exception {
    String filter = "WHERE %s IS NOT NULL".formatted(column);
    DataRepoResponse<SnapshotPreviewModel> response =
        retrieveSnapshotPreviewByIdRaw(user, snapshotId, table, 0, 1, filter, null);
    SnapshotPreviewModel validated =
        validateResponse(response, "snapshot preview for DRS ID", HttpStatus.OK, null);

    assertThat("Got one row", validated.getResult(), hasSize(1));

    String drsUri = (String) ((LinkedHashMap) validated.getResult().get(0)).get(column);
    assertThat("DRS URI was found", drsUri, notNullValue());

    return DrsService.getLastNameFromPath(drsUri);
  }

  public Object retrieveFirstResultSnapshotPreviewById(
      TestConfiguration.User user,
      UUID snapshotId,
      String table,
      int offset,
      int limit,
      String filter)
      throws Exception {
    return retrieveSnapshotPreviewById(user, snapshotId, table, offset, limit, filter)
        .getResult()
        .get(0);
  }

  public SnapshotPreviewModel retrieveSnapshotPreviewById(
      TestConfiguration.User user,
      UUID snapshotId,
      String table,
      int offset,
      int limit,
      String filter)
      throws Exception {
    return retrieveSnapshotPreviewById(user, snapshotId, table, offset, limit, filter, null);
  }

  public SnapshotPreviewModel retrieveSnapshotPreviewById(
      TestConfiguration.User user,
      UUID snapshotId,
      String table,
      int offset,
      int limit,
      String filter,
      String sort)
      throws Exception {
    DataRepoResponse<SnapshotPreviewModel> response =
        retrieveSnapshotPreviewByIdRaw(user, snapshotId, table, offset, limit, filter, sort);
    return validateResponse(response, "snapshot data", HttpStatus.OK, null);
  }

  private DataRepoResponse<SnapshotPreviewModel> retrieveSnapshotPreviewByIdRaw(
      TestConfiguration.User user,
      UUID snapshotId,
      String table,
      Integer offset,
      Integer limit,
      String filter,
      String sort)
      throws Exception {
    String url = "/api/repository/v1/snapshots/%s/data/%s".formatted(snapshotId, table);

    offset = Objects.requireNonNullElse(offset, 0);
    limit = Objects.requireNonNullElse(limit, 10);
    String queryParams = "?offset=%s&limit=%s".formatted(offset, limit);

    if (filter != null) {
      queryParams += "&filter=%s".formatted(filter);
    }
    if (sort != null) {
      queryParams += "&sort=%s".formatted(sort);
    }
    return dataRepoClient.get(user, url + queryParams, new TypeReference<>() {});
  }

  public List<String> getRowIds(
      TestConfiguration.User user, DatasetModel dataset, String tableName, int limitRowsReturned)
      throws Exception {
    List<Object> dataModel =
        retrieveDatasetData(user, dataset.getId(), tableName, 0, limitRowsReturned, null)
            .getResult();
    assertThat("got right num of row ids back", dataModel.size(), equalTo(limitRowsReturned));
    return dataModel.stream()
        .map(r -> ((LinkedHashMap) r).get(PDAO_ROW_ID_COLUMN).toString())
        .toList();
  }

  public void assertSnapshotTableCount(
      TestConfiguration.User user, SnapshotModel snapshotModel, String tableName, int n)
      throws Exception {
    SnapshotPreviewModel previewModel =
        retrieveSnapshotPreviewById(user, snapshotModel.getId(), tableName, 0, n + 1, null);
    // since we're not filtering and the results should not be limited, all three of these should be
    // equal
    int rowCount = previewModel.getResult().size();
    assertThat("Results row count matches expected row count", rowCount, equalTo(n));
    int totalRowCount = previewModel.getTotalRowCount();
    assertThat("Total row count matches expected row count", totalRowCount, equalTo(n));
    int filteredRowCount = previewModel.getFilteredRowCount();
    assertThat("Filtered row count matches expected row count", filteredRowCount, equalTo(n));
  }

  public void assertDatasetTableCount(
      TestConfiguration.User user, DatasetModel dataset, String tableName, int n) throws Exception {
    DatasetDataModel datasetDataModel =
        retrieveDatasetData(user, dataset.getId(), tableName, 0, n + 1, null);
    // since we're not filtering and the results should not be limited, all three of these should be
    // equal
    int rowCount = datasetDataModel.getResult().size();
    assertThat("Results row count matches expected row count", rowCount, equalTo(n));
    int totalRowCount = datasetDataModel.getTotalRowCount();
    assertThat("Total row count matches expected row count", totalRowCount, equalTo(n));
    int filteredRowCount = datasetDataModel.getFilteredRowCount();
    assertThat("Filtered row count matches expected row count", filteredRowCount, equalTo(n));
  }

  public List<Map<String, List<String>>> transformStringResults(
      TestConfiguration.User user, DatasetModel dataset, String tableName) throws Exception {
    List<Object> dataModel =
        retrieveDatasetData(user, dataset.getId(), tableName, 0, 100, null).getResult();
    List<String> columnNamesFromResults =
        ((LinkedHashMap) dataModel.get(0)).keySet().stream().toList();
    List<ColumnModel> columns =
        dataset.getSchema().getTables().stream()
            .filter(t -> t.getName().equals(tableName))
            .findFirst()
            .get()
            .getColumns();
    List<Map<String, List<String>>> result = new ArrayList<>();
    for (Object val : dataModel) {
      Map<String, List<String>> transformed = new HashMap<>();
      for (String columnName : columnNamesFromResults) {
        String colVal = ((LinkedHashMap) val).get(columnName).toString();
        final List<String> values;
        Optional<ColumnModel> columnModel =
            columns.stream().filter(c -> c.getName().equals(columnName)).findFirst();
        if (columnModel.isPresent() && columnModel.get().isArrayOf()) {
          String subStringVal = colVal.substring(1, colVal.length() - 1);
          values = Arrays.stream(subStringVal.split(",")).toList();
        } else {
          values = List.of(colVal);
        }
        transformed.put(columnName, values);
      }
      result.add(transformed);
    }
    return result;
  }

  public void retrieveDatasetDataExpectFailure(
      TestConfiguration.User user,
      UUID datasetId,
      String table,
      int offset,
      int limit,
      String filter,
      HttpStatus expectedStatus)
      throws Exception {
    DataRepoResponse<DatasetDataModel> response =
        retrieveDatasetDataByIdRaw(user, datasetId, table, offset, limit, filter, null, null);
    assertThat(
        "retrieve dataset data by Id should fail",
        response.getStatusCode(),
        equalTo(expectedStatus));
  }

  public DatasetDataModel retrieveDatasetData(
      TestConfiguration.User user,
      UUID datasetId,
      String table,
      int offset,
      int limit,
      String filter)
      throws Exception {
    return retrieveDatasetData(user, datasetId, table, offset, limit, filter, null, null);
  }

  public DatasetDataModel retrieveDatasetData(
      TestConfiguration.User user,
      UUID datasetId,
      String table,
      int offset,
      int limit,
      String filter,
      String sort,
      String direction)
      throws Exception {
    DataRepoResponse<DatasetDataModel> response =
        retrieveDatasetDataByIdRaw(user, datasetId, table, offset, limit, filter, sort, direction);
    return validateResponse(response, "dataset data", HttpStatus.OK, null);
  }

  private DataRepoResponse<DatasetDataModel> retrieveDatasetDataByIdRaw(
      TestConfiguration.User user,
      UUID datasetId,
      String table,
      Integer offset,
      Integer limit,
      String filter,
      String sort,
      String direction)
      throws Exception {
    String url = "/api/repository/v1/datasets/%s/data/%s".formatted(datasetId, table);

    offset = Objects.requireNonNullElse(offset, 0);
    limit = Objects.requireNonNullElse(limit, 10);
    String queryParams = "?offset=%s&limit=%s".formatted(offset, limit);

    if (filter != null) {
      queryParams += "&filter=%s".formatted(filter);
    }
    if (sort != null) {
      queryParams += "&sort=%s".formatted(sort);
    }
    if (direction != null) {
      queryParams += "&direction=%s".formatted(direction);
    }
    return dataRepoClient.get(user, url + queryParams, new TypeReference<>() {});
  }

  public void assertFailToGetSnapshot(TestConfiguration.User user, UUID snapshotId)
      throws Exception {
    DataRepoResponse<SnapshotModel> response = getSnapshotRaw(user, snapshotId, null);
    assertThat(
        "snapshot is not successfully retrieved",
        response.getStatusCode(),
        oneOf(HttpStatus.FORBIDDEN, HttpStatus.NOT_FOUND));
  }

  public DataRepoResponse<EnumerateSnapshotModel> enumerateSnapshotsByDatasetIdsRaw(
      TestConfiguration.User user, List<UUID> datasetIds) throws Exception {
    String datasetIdsString;
    List<String> datasetIdsQuery =
        ListUtils.emptyIfNull(datasetIds).stream()
            .map(id -> "datasetIds=" + id)
            .collect(Collectors.toList());
    datasetIdsString = StringUtils.join(datasetIdsQuery, "&");
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots?" + datasetIdsString + "&sort=created_date&direction=desc",
        new TypeReference<>() {});
  }

  public EnumerateSnapshotModel enumerateSnapshotsByDatasetIds(
      TestConfiguration.User user, List<UUID> datasetIds) throws Exception {
    DataRepoResponse<EnumerateSnapshotModel> response =
        enumerateSnapshotsByDatasetIdsRaw(user, datasetIds);
    return validateResponse(response, "snapshot enumerate by dataset ids", HttpStatus.OK, null);
  }

  public DataRepoResponse<EnumerateSnapshotModel> enumerateSnapshotsRaw(TestConfiguration.User user)
      throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots?sort=created_date&direction=desc",
        new TypeReference<>() {});
  }

  public EnumerateSnapshotModel enumerateSnapshots(TestConfiguration.User user) throws Exception {
    DataRepoResponse<EnumerateSnapshotModel> response = enumerateSnapshotsRaw(user);
    return validateResponse(response, "snapshot enumerate", HttpStatus.OK, null);
  }

  public void deleteSnapshot(TestConfiguration.User user, UUID snapshotId) throws Exception {
    DataRepoResponse<DeleteResponseModel> deleteResponse = deleteSnapshotLog(user, snapshotId);
    assertGoodDeleteResponse(deleteResponse);
  }

  public DataRepoResponse<DeleteResponseModel> deleteSnapshotLog(
      TestConfiguration.User user, UUID snapshotId) throws Exception {

    DataRepoResponse<JobModel> jobResponse = deleteSnapshotLaunch(user, snapshotId);
    assertTrue("snapshot delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "snapshot delete launch response is present", jobResponse.getResponseObject().isPresent());

    return dataRepoClient.waitForResponseLog(user, jobResponse, new TypeReference<>() {});
  }

  public DataRepoResponse<JobModel> deleteSnapshotLaunch(
      TestConfiguration.User user, UUID snapshotId) throws Exception {
    return dataRepoClient.delete(
        user, "/api/repository/v1/snapshots/" + snapshotId, new TypeReference<>() {});
  }

  private void assertGoodDeleteResponse(DataRepoResponse<DeleteResponseModel> deleteResponse) {

    assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
    DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
    assertTrue(
        "Valid delete response",
        (deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED
            || deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
  }

  public DataRepoResponse<SnapshotExportResponseModel> exportSnapshotLog(
      TestConfiguration.User user,
      UUID snapshotId,
      boolean resolveGsPaths,
      boolean validatePkUniqueness)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse =
        exportSnapshot(user, snapshotId, resolveGsPaths, validatePkUniqueness);
    assertTrue("snapshot export launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "snapshot export launch response is present", jobResponse.getResponseObject().isPresent());

    return dataRepoClient.waitForResponseLog(user, jobResponse, new TypeReference<>() {});
  }

  public DataRepoResponse<JobModel> exportSnapshot(
      TestConfiguration.User user,
      UUID snapshotId,
      boolean resolveGsPaths,
      boolean validatePkUniqueness)
      throws Exception {
    return dataRepoClient.get(
        user,
        String.format(
            "/api/repository/v1/snapshots/%s/export?exportGsPaths=%s&validatePrimaryKeyUniqueness=%s",
            snapshotId, resolveGsPaths, validatePkUniqueness),
        new TypeReference<>() {});
  }

  public DataRepoResponse<JobModel> exportSnapshotResolveGsPaths(
      TestConfiguration.User user, UUID snapshotId) throws Exception {
    return dataRepoClient.get(
        user,
        String.format("/api/repository/v1/snapshots/%s/export?exportGsPaths=true", snapshotId),
        new TypeReference<>() {});
  }

  public DatasetModel updateSchema(
      TestConfiguration.User user, UUID datasetId, DatasetSchemaUpdateModel request)
      throws Exception {
    DataRepoResponse<JobModel> jobResponse = updateSchemaRaw(user, datasetId, request);
    assertTrue("update schema succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue("update schema response is present", jobResponse.getResponseObject().isPresent());
    DataRepoResponse<DatasetModel> updateResponse =
        dataRepoClient.waitForResponse(user, jobResponse, new TypeReference<>() {});
    return validateResponse(updateResponse, "update schema", HttpStatus.OK, jobResponse);
  }

  public DataRepoResponse<JobModel> updateSchemaRaw(
      TestConfiguration.User user, UUID datasetId, DatasetSchemaUpdateModel request)
      throws Exception {
    String ingestBody = TestUtils.mapToJson(request);
    return dataRepoClient.post(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/updateSchema",
        ingestBody,
        new TypeReference<>() {});
  }

  public DataRepoResponse<JobModel> ingestJsonDataLaunch(
      TestConfiguration.User user, UUID datasetId, IngestRequestModel request) throws Exception {
    String ingestBody = TestUtils.mapToJson(request);
    return dataRepoClient.post(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/ingest",
        ingestBody,
        new TypeReference<>() {});
  }

  public ErrorModel ingestJsonDataFailure(
      TestConfiguration.User user, UUID datasetId, IngestRequestModel request) throws Exception {
    DataRepoResponse<JobModel> jobResponse = ingestJsonDataLaunch(user, datasetId, request);
    assertTrue("ingest data launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "ingest data launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<ErrorModel> response =
        dataRepoClient.waitForResponse(user, jobResponse, new TypeReference<>() {});
    assertFalse("ingest data is failure", response.getStatusCode().is2xxSuccessful());

    assertTrue("ingest data error response is present", response.getErrorObject().isPresent());
    return response.getErrorObject().get();
  }

  public IngestResponseModel ingestJsonData(
      TestConfiguration.User user, UUID datasetId, IngestRequestModel request) throws Exception {
    DataRepoResponse<IngestResponseModel> response = ingestJsonDataRaw(user, datasetId, request);

    assertThat("ingestOne is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("ingestOne response is present", response.getResponseObject().isPresent());

    IngestResponseModel ingestResponse = response.getResponseObject().get();
    assertThat("no bad sample rows", ingestResponse.getBadRowCount(), equalTo(0L));
    return ingestResponse;
  }

  public DataRepoResponse<IngestResponseModel> ingestJsonDataRaw(
      TestConfiguration.User user, UUID datasetId, IngestRequestModel request) throws Exception {
    DataRepoResponse<JobModel> launchResp = ingestJsonDataLaunch(user, datasetId, request);
    assertTrue("ingest launch succeeded", launchResp.getStatusCode().is2xxSuccessful());
    assertTrue("ingest launch response is present", launchResp.getResponseObject().isPresent());
    return dataRepoClient.waitForResponse(user, launchResp, new TypeReference<>() {});
  }

  public DataRepoResponse<JobModel> ingestFileLaunch(
      TestConfiguration.User user,
      UUID datasetId,
      UUID profileId,
      String sourceGsPath,
      String targetPath)
      throws Exception {

    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(sourceGsPath)
            .profileId(profileId)
            .description(null)
            .mimeType("application/octet-string")
            .targetPath(targetPath);

    String json = TestUtils.mapToJson(fileLoadModel);

    return dataRepoClient.post(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/files",
        json,
        new TypeReference<>() {});
  }

  public FileModel ingestFile(
      TestConfiguration.User user,
      UUID datasetId,
      UUID profileId,
      String sourceGsPath,
      String targetPath)
      throws Exception {
    DataRepoResponse<JobModel> resp =
        ingestFileLaunch(user, datasetId, profileId, sourceGsPath, targetPath);
    assertTrue("ingest launch succeeded", resp.getStatusCode().is2xxSuccessful());
    assertTrue("ingest launch response is present", resp.getResponseObject().isPresent());

    DataRepoResponse<FileModel> response =
        dataRepoClient.waitForResponse(user, resp, new TypeReference<>() {});
    return assertSuccessful(response, "ingestFile failed");
  }

  public DataRepoResponse<JobModel> bulkLoadArrayRaw(
      TestConfiguration.User user, UUID datasetId, BulkLoadArrayRequestModel requestModel)
      throws Exception {

    String json = TestUtils.mapToJson(requestModel);

    DataRepoResponse<JobModel> launchResponse =
        dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/files/bulk/array",
            json,
            new TypeReference<>() {});
    assertTrue("bulkLoadArray launch succeeded", launchResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "bulkloadArray launch response is present", launchResponse.getResponseObject().isPresent());
    return launchResponse;
  }

  public BulkLoadArrayResultModel bulkLoadArray(
      TestConfiguration.User user, UUID datasetId, BulkLoadArrayRequestModel requestModel)
      throws Exception {

    DataRepoResponse<JobModel> launchResponse = bulkLoadArrayRaw(user, datasetId, requestModel);

    DataRepoResponse<BulkLoadArrayResultModel> response =
        dataRepoClient.waitForResponse(user, launchResponse, new TypeReference<>() {});
    return assertSuccessful(response, "bulkLoadArray failed");
  }

  public DataRepoResponse<JobModel> bulkLoadRaw(
      TestConfiguration.User user, UUID datasetId, BulkLoadRequestModel requestModel)
      throws Exception {
    String json = TestUtils.mapToJson(requestModel);
    DataRepoResponse<JobModel> launchResponse =
        dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/files/bulk",
            json,
            new TypeReference<>() {});
    assertTrue("bulkLoad launch succeeded", launchResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "bulkload launch response is present", launchResponse.getResponseObject().isPresent());
    return launchResponse;
  }

  public BulkLoadResultModel bulkLoad(
      TestConfiguration.User user, UUID datasetId, BulkLoadRequestModel requestModel)
      throws Exception {

    DataRepoResponse<JobModel> launchResponse = bulkLoadRaw(user, datasetId, requestModel);

    DataRepoResponse<BulkLoadResultModel> response =
        dataRepoClient.waitForResponse(user, launchResponse, new TypeReference<>() {});
    return assertSuccessful(response, "bulkLoad failed");
  }

  public BulkLoadHistoryModelList getLoadHistory(
      TestConfiguration.User user, UUID datasetId, String loadTag, int offset, int limit)
      throws Exception {
    DataRepoResponse<BulkLoadHistoryModelList> response =
        dataRepoClient.get(
            user,
            "/api/repository/v1/datasets/"
                + datasetId
                + "/files/bulk/"
                + loadTag
                + "?offset="
                + offset
                + "&limit="
                + limit,
            new TypeReference<>() {});
    return assertSuccessful(response, "getLoadHistory failed");
  }

  private <T> T assertSuccessful(DataRepoResponse<T> response, String errMsg) {
    if (response.getStatusCode().is2xxSuccessful()) {
      assertThat("getLoadHistory is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
      assertTrue("getLoadHistory response is present", response.getResponseObject().isPresent());
      return response.getResponseObject().get();
    }
    ErrorModel errorModel = response.getErrorObject().orElse(null);
    throw new AssertionError(errMsg + "; Error: " + errorModel);
  }

  public DataRepoResponse<FileModel> getFileByIdRaw(
      TestConfiguration.User user, UUID datasetId, String fileId) throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/files/" + fileId,
        new TypeReference<>() {});
  }

  public FileModel getFileById(TestConfiguration.User user, UUID datasetId, String fileId)
      throws Exception {
    DataRepoResponse<FileModel> response = getFileByIdRaw(user, datasetId, fileId);
    assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("file get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<FileModel> getFileByNameRaw(
      TestConfiguration.User user, UUID datasetId, String path) throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/filesystem/objects?path=" + path,
        new TypeReference<>() {});
  }

  public FileModel getFileByName(TestConfiguration.User user, UUID datasetId, String path)
      throws Exception {
    DataRepoResponse<FileModel> response = getFileByNameRaw(user, datasetId, path);
    assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("file get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<FileModel> getSnapshotFileByIdRaw(
      TestConfiguration.User user, UUID snapshotId, String fileId) throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots/" + snapshotId + "/files/" + fileId,
        new TypeReference<>() {});
  }

  public FileModel getSnapshotFileById(TestConfiguration.User user, UUID snapshotId, String fileId)
      throws Exception {
    DataRepoResponse<FileModel> response = getSnapshotFileByIdRaw(user, snapshotId, fileId);
    assertThat("file is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("file get response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<FileModel> getSnapshotFileByNameRaw(
      TestConfiguration.User user, UUID snapshotId, String path) throws Exception {
    return dataRepoClient.get(
        user,
        "/api/repository/v1/snapshots/" + snapshotId + "/filesystem/objects?path=" + path,
        new TypeReference<>() {});
  }

  public FileModel getSnapshotFileByName(TestConfiguration.User user, UUID snapshotId, String path)
      throws Exception {
    DataRepoResponse<FileModel> response = getSnapshotFileByNameRaw(user, snapshotId, path);
    return validateResponse(response, "retrieving snapshot file", HttpStatus.OK, null);
  }

  public DataRepoResponse<JobModel> deleteFileLaunch(
      TestConfiguration.User user, UUID datasetId, String fileId) throws Exception {
    return dataRepoClient.delete(
        user,
        "/api/repository/v1/datasets/" + datasetId + "/files/" + fileId,
        new TypeReference<>() {});
  }

  public void deleteFile(TestConfiguration.User user, UUID datasetId, String fileId)
      throws Exception {
    DataRepoResponse<JobModel> launchResp = deleteFileLaunch(user, datasetId, fileId);
    assertTrue("delete launch succeeded", launchResp.getStatusCode().is2xxSuccessful());
    assertTrue("delete launch response is present", launchResp.getResponseObject().isPresent());
    DataRepoResponse<DeleteResponseModel> deleteResponse =
        dataRepoClient.waitForResponse(user, launchResp, new TypeReference<>() {});
    assertGoodDeleteResponse(deleteResponse);
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  public DrsResponse<DRSObject> drsGetObjectRaw(TestConfiguration.User user, String drsObjectId)
      throws Exception {
    return dataRepoClient.drsGet(
        user, "/ga4gh/drs/v1/objects/" + drsObjectId, new TypeReference<>() {});
  }

  public DrsResponse<bio.terra.model.DRSAccessURL> getObjectAccessUrl(
      TestConfiguration.User user, String drsObjectId, String accessId) throws Exception {
    return dataRepoClient.drsGet(
        user,
        "/ga4gh/drs/v1/objects/" + drsObjectId + "/access/" + accessId,
        new TypeReference<>() {});
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  public DRSObject drsGetObject(TestConfiguration.User user, String drsObjectId) throws Exception {
    DrsResponse<DRSObject> response = drsGetObjectRaw(user, drsObjectId);
    assertThat(
        "object is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("object get response is present", response.getResponseObject().isPresent());
    return validateResponse(response, "retrieving Drs object", HttpStatus.OK);
  }

  public Storage getStorage(String token) {
    GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
    StorageOptions storageOptions =
        StorageOptions.newBuilder().setCredentials(googleCredentials).build();
    return storageOptions.getService();
  }

  public IngestRequestModel buildSimpleIngest(String table, String filename) throws Exception {
    String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + filename;
    return new IngestRequestModel()
        .format(IngestRequestModel.FormatEnum.JSON)
        .ignoreUnknownValues(false)
        .maxBadRecords(0)
        .table(table)
        .path(gsPath);
  }

  public IngestRequestModel buildSimpleIngest(String table, List<Map<String, Object>> data)
      throws Exception {
    return new IngestRequestModel()
        .format(IngestRequestModel.FormatEnum.ARRAY)
        .ignoreUnknownValues(false)
        .maxBadRecords(0)
        .table(table)
        .records(Arrays.asList(data.toArray()));
  }

  // Transaction methods
  public TransactionModel openTransaction(
      TestConfiguration.User user, UUID datasetId, TransactionCreateModel requestModel)
      throws Exception {
    String json = TestUtils.mapToJson(requestModel);
    DataRepoResponse<JobModel> post =
        dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/transactions",
            json,
            new TypeReference<>() {});
    return waitForTransactionCreate(user, post);
  }

  private TransactionModel waitForTransactionCreate(
      TestConfiguration.User user, DataRepoResponse<JobModel> jobResponse) throws Exception {
    assertTrue(
        "transaction create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "transaction create launch response is present",
        jobResponse.getResponseObject().isPresent());

    DataRepoResponse<TransactionModel> response =
        dataRepoClient.waitForResponseLog(user, jobResponse, new TypeReference<>() {});
    logger.info("Response was: {}", response);
    return validateResponse(response, "transaction create", HttpStatus.CREATED, jobResponse);
  }

  public TransactionModel closeTransaction(
      TestConfiguration.User user,
      UUID datasetId,
      UUID transactionId,
      TransactionCloseModel requestModel)
      throws Exception {
    String json = TestUtils.mapToJson(requestModel);
    DataRepoResponse<JobModel> post =
        dataRepoClient.post(
            user,
            "/api/repository/v1/datasets/" + datasetId + "/transactions/" + transactionId,
            json,
            new TypeReference<>() {});
    return waitForTransactionClose(user, post);
  }

  private TransactionModel waitForTransactionClose(
      TestConfiguration.User user, DataRepoResponse<JobModel> jobResponse) throws Exception {
    assertTrue("transaction close launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "transaction close launch response is present",
        jobResponse.getResponseObject().isPresent());

    DataRepoResponse<TransactionModel> response =
        dataRepoClient.waitForResponseLog(user, jobResponse, new TypeReference<>() {});
    logger.info("Response was: {}", response);
    return validateResponse(response, "transaction close", HttpStatus.OK, null);
  }

  // Configuration methods

  public DataRepoResponse<ConfigModel> getConfig(TestConfiguration.User user, String configName)
      throws Exception {
    return dataRepoClient.get(
        user, "/api/repository/v1/configs/" + configName, new TypeReference<>() {});
  }

  public DataRepoResponse<ConfigModel> setFault(
      TestConfiguration.User user, String configName, boolean enable) throws Exception {
    ConfigEnableModel configEnableModel = new ConfigEnableModel().enabled(enable);
    String json = TestUtils.mapToJson(configEnableModel);
    return dataRepoClient.put(
        user, "/api/repository/v1/configs/" + configName, json, new TypeReference<>() {});
  }

  public DataRepoResponse<Void> resetConfig(TestConfiguration.User user) throws Exception {
    return dataRepoClient.put(
        user, "/api/repository/v1/configs/reset", "{}", new TypeReference<Void>() {});
  }

  public DataRepoResponse<ConfigListModel> setConfigListRaw(
      TestConfiguration.User user, ConfigGroupModel configGroup) throws Exception {
    String json = TestUtils.mapToJson(configGroup);
    return dataRepoClient.put(user, "/api/repository/v1/configs", json, new TypeReference<>() {});
  }

  public ConfigListModel setConfigList(TestConfiguration.User user, ConfigGroupModel configGroup)
      throws Exception {
    DataRepoResponse<ConfigListModel> response = setConfigListRaw(user, configGroup);
    assertThat("setConfigList is successfully", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("setConfigList response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<ConfigListModel> getConfigListRaw(TestConfiguration.User user)
      throws Exception {
    return dataRepoClient.get(user, "/api/repository/v1/configs", new TypeReference<>() {});
  }

  public ConfigListModel getConfigList(TestConfiguration.User user) throws Exception {
    DataRepoResponse<ConfigListModel> response = getConfigListRaw(user);
    assertThat("getConfigList is successfully", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("getConfigList response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public void assertCombinedIngestCorrect(
      IngestResponseModel ingestResponse, TestConfiguration.User user) {
    var loadResult = ingestResponse.getLoadResult();

    assertThat("file_load_results are in the response", loadResult, notNullValue());

    assertThat(
        "all 4 files got ingested", loadResult.getLoadSummary().getSucceededFiles(), equalTo(4));

    var fileModels =
        loadResult.getLoadFileResults().stream()
            .map(BulkLoadFileResultModel::getTargetPath)
            .map(
                path -> {
                  try {
                    return getFileByName(user, ingestResponse.getDatasetId(), path);
                  } catch (Exception e) {
                    throw new RuntimeException(
                        "Unable to find file by name. TargetPath: "
                            + path
                            + "; DatasetId: "
                            + ingestResponse.getDatasetId(),
                        e);
                  }
                })
            .flatMap(file -> Optional.ofNullable(file).stream())
            .collect(Collectors.toList());

    var fileIds =
        loadResult.getLoadFileResults().stream()
            .map(BulkLoadFileResultModel::getFileId)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    var retrievedFileIds =
        fileModels.stream().map(FileModel::getFileId).collect(Collectors.toList());

    assertThat("The files were ingested correctly", fileIds, equalTo(retrievedFileIds));
  }

  private <T> T validateResponse(
      DataRepoResponse<T> response,
      String action,
      HttpStatus expectedCode,
      DataRepoResponse<JobModel> jobResponse) {
    try {
      assertThat(
          String.format("%s is successful", action),
          response.getStatusCode(),
          equalTo(expectedCode));
      assertTrue(
          String.format("%s response is present", action),
          response.getResponseObject().isPresent());
      return response.getResponseObject().get();
    } catch (AssertionError e) {
      String jobId =
          Optional.ofNullable(jobResponse)
              .flatMap(DataRepoResponse::getResponseObject)
              .map(JobModel::getId)
              .orElse(null);

      String addedLink =
          (testConfig.getIntegrationServerNumber() != null)
              ? String.format("%nFor more information, see: %s", getStackdriverUrl(jobId))
              : "no int server number";
      throw new AssertionError(
          String.format("Error validating %s.  Got response: %s%s", action, response, addedLink));
    }
  }

  private <T> T validateResponse(DrsResponse<T> response, String action, HttpStatus expectedCode) {
    try {
      assertThat(
          String.format("%s is successful", action),
          response.getStatusCode(),
          equalTo(expectedCode));
      assertTrue(
          String.format("%s response is present", action),
          response.getResponseObject().isPresent());
      return response.getResponseObject().get();
    } catch (AssertionError e) {
      throw new AssertionError(
          String.format("Error validating %s.  Got response: %s", action, response));
    }
  }

  private String getStackdriverUrl(final String jobId) {
    String query =
        URLEncoder.encode(
            new ST(QUERY_TEMPLATE)
                .add("intNumber", testConfig.getIntegrationServerNumber())
                .add("flightId", jobId)
                .add("hasFlightId", !StringUtils.isEmpty(jobId))
                .render(),
            StandardCharsets.UTF_8);
    return "https://console.cloud.google.com/logs/query;"
        + query
        + ";cursorTimestamp="
        + Instant.now().minus(Duration.ofSeconds(30)).toString()
        + "?project="
        + testConfig.getGoogleProjectId();
  }

  // Jobs

  public void getJobSuccess(String jobId, TestConfiguration.User user) throws Exception {
    DataRepoResponse<JobModel> jobIdResponse = getJobIdRaw(jobId, user);
    try {
      assertTrue("job launch succeeded", jobIdResponse.getStatusCode().is2xxSuccessful());
      assertTrue("job launch response is present", jobIdResponse.getResponseObject().isPresent());
    } catch (AssertionError e) {
      throw new AssertionError(String.format("Job launch failed. Got response: %s", jobIdResponse));
    }
  }

  public DataRepoResponse<JobModel> getJobIdRaw(String jobId, TestConfiguration.User user)
      throws Exception {
    return dataRepoClient.get(user, "/api/repository/v1/jobs/" + jobId, new TypeReference<>() {});
  }

  public List<JobModel> enumerateJobs(TestConfiguration.User user, Integer offset, Integer limit)
      throws Exception {
    DataRepoResponse<List<JobModel>> response = enumerateJobsRaw(user, offset, limit);
    assertThat("enumerate jobs is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
    assertTrue("enumerate jobs response is present", response.getResponseObject().isPresent());
    return response.getResponseObject().get();
  }

  public DataRepoResponse<List<JobModel>> enumerateJobsRaw(
      TestConfiguration.User user, Integer offset, Integer limit) throws Exception {
    String queryParams =
        "?offset=%s&limit=%s"
            .formatted(
                Objects.requireNonNullElse(offset, 0), Objects.requireNonNullElse(limit, 10));
    return dataRepoClient.get(
        user, "/api/repository/v1/jobs" + queryParams, new TypeReference<>() {});
  }
}
