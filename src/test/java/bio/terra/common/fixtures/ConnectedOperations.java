package bio.terra.common.fixtures;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSObject;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetDaoUtils;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.tabulardata.DataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import com.azure.data.tables.TableServiceClient;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

// Common code for creating and deleting datasets and snapshots via MockMvc
// and tracking what is created so it can be deleted.
@Component
public class ConnectedOperations {
  private static final Logger logger = LoggerFactory.getLogger(ConnectedOperations.class);
  // The policy email must be a real google group, otherwise requests that
  // update bigquery dataset policies will fail.
  public static final String POLICY_EMAIL = "jadeteam@broadinstitute.org";

  private final MockMvc mvc;
  private final JsonLoader jsonLoader;
  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private final ConnectedTestConfiguration testConfig;

  private final List<UUID> createdSnapshotIds = new ArrayList<>();
  private final List<UUID> createdDatasetIds = new ArrayList<>();
  private final List<UUID> createdProfileIds = new ArrayList<>();
  private final List<String[]> createdFileIds =
      new ArrayList<>(); // [0] is datasetid, [1] is fileid
  private final List<String> createdBuckets = new ArrayList<>();
  private final List<String> createdScratchFiles = new ArrayList<>();

  @Autowired
  public ConnectedOperations(
      MockMvc mvc, JsonLoader jsonLoader, ConnectedTestConfiguration testConfig) {
    this.mvc = mvc;
    this.jsonLoader = jsonLoader;
    this.testConfig = testConfig;
  }

  private static Map<UUID, Set<IamRole>> uuidsToAuthMap(List<UUID> uuids) {
    return uuids.stream()
        .collect(Collectors.toMap(Function.identity(), x -> Set.of(IamRole.READER)));
  }

  public void stubOutSamCalls(IamProviderInterface samService) throws Exception {
    Map<IamRole, String> snapshotPolicies =
        new EnumMap<>(
            Map.of(
                IamRole.STEWARD, POLICY_EMAIL,
                IamRole.READER, POLICY_EMAIL));
    Map<IamRole, String> datasetPolicies =
        new EnumMap<>(
            Map.of(
                IamRole.CUSTODIAN, POLICY_EMAIL,
                IamRole.STEWARD, POLICY_EMAIL,
                IamRole.SNAPSHOT_CREATOR, POLICY_EMAIL));

    when(samService.createSnapshotResource(any(), any(), any(), any()))
        .thenReturn(snapshotPolicies);
    when(samService.isAuthorized(any(), any(), any(), any())).thenReturn(Boolean.TRUE);
    when(samService.createDatasetResource(any(), any(), any())).thenReturn(datasetPolicies);
    when(samService.listActions(any(), eq(IamResourceType.DATASET), any()))
        .thenReturn(List.of(IamAction.READ_DATASET.toString()));

    when(samService.retrievePolicyEmails(any(), eq(IamResourceType.DATASET), any()))
        .thenReturn(datasetPolicies);

    // when asked what datasets/snapshots the caller has access to, return all the
    // datasets/snapshots contained
    // in the bookkeeping lists (createdDatasetIds/createdDatasetIds) in this class.
    when(samService.listAuthorizedResources(any(), eq(IamResourceType.DATASET)))
        .thenAnswer(invocation -> uuidsToAuthMap(createdDatasetIds));
    when(samService.listAuthorizedResources(any(), eq(IamResourceType.DATASNAPSHOT)))
        .thenAnswer(invocation -> uuidsToAuthMap(createdSnapshotIds));
    doNothing().when(samService).deleteSnapshotResource(any(), any());
    doNothing().when(samService).deleteDatasetResource(any(), any());

    // Mock the billing profile calls
    when(samService.listAuthorizedResources(any(), eq(IamResourceType.SPEND_PROFILE)))
        .thenAnswer(invocation -> uuidsToAuthMap(createdProfileIds));
    when(samService.hasAnyActions(any(), eq(IamResourceType.SPEND_PROFILE), any()))
        .thenReturn(true);

    doNothing().when(samService).createProfileResource(any(), any());
    doNothing().when(samService).deleteProfileResource(any(), any());
  }

  /**
   * Creating a dataset through the http layer causes a dataset create flight to run, creating
   * metadata and primary data to be modified.
   *
   * @param resourcePath path to json used for a dataset create request
   * @return summary of the dataset created
   */
  public DatasetSummaryModel createDataset(BillingProfileModel profileModel, String resourcePath)
      throws Exception {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(profileModel.getId())
        .cloudPlatform(profileModel.getCloudPlatform());

    return createDataset(datasetRequest);
  }

  public DatasetSummaryModel createDataset(DatasetRequestModel datasetRequest) throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(datasetRequest)))
            .andReturn();
    MockHttpServletResponse response = validateJobModelAndWait(result);
    DatasetSummaryModel datasetSummaryModel =
        handleSuccessCase(response, DatasetSummaryModel.class);
    addDataset(datasetSummaryModel.getId());
    return datasetSummaryModel;
  }

  public ErrorModel createDatasetExpectError(
      DatasetRequestModel datasetRequest, HttpStatus expectedStatus) throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(datasetRequest)))
            .andReturn();
    MockHttpServletResponse response = validateJobModelAndWait(result);
    return handleFailureCase(response, expectedStatus);
  }

  public BillingProfileModel createProfileForAccount(String billingAccountId) throws Exception {
    BillingProfileRequestModel profileRequestModel =
        ProfileFixtures.randomBillingProfileRequest().billingAccountId(billingAccountId);
    return createProfile(profileRequestModel);
  }

  public BillingProfileModel createProfile(BillingProfileRequestModel profileRequestModel)
      throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(profileRequestModel)))
            .andReturn();

    MockHttpServletResponse response = validateJobModelAndWait(result);
    BillingProfileModel billingProfileModel =
        handleSuccessCase(response, BillingProfileModel.class);
    addProfile(billingProfileModel.getId());
    return billingProfileModel;
  }

  public ErrorModel createProfileExpectError(
      BillingProfileRequestModel profileRequestModel, HttpStatus expectedStatus) throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(profileRequestModel)))
            .andReturn();

    return handleFailureCase(result.getResponse(), expectedStatus);
  }

  public BillingProfileModel getProfileById(UUID profileId) throws Exception {
    MvcResult result =
        mvc.perform(
                get("/api/resources/v1/profiles/" + profileId)
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();

    return TestUtils.mapFromJson(
        result.getResponse().getContentAsString(), BillingProfileModel.class);
  }

  public BillingProfileModel updateProfile(BillingProfileUpdateModel profileRequestModel)
      throws Exception {
    MvcResult result =
        mvc.perform(
                put("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(profileRequestModel)))
            .andReturn();

    MockHttpServletResponse response = validateJobModelAndWait(result);
    return handleSuccessCase(response, BillingProfileModel.class);
  }

  public ErrorModel updateProfileExpectError(
      BillingProfileUpdateModel profileRequestModel, HttpStatus expectedStatus) throws Exception {
    MvcResult result =
        mvc.perform(
                put("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(profileRequestModel)))
            .andReturn();

    return handleFailureCase(result.getResponse(), expectedStatus);
  }

  public SnapshotSummaryModel createSnapshot(
      DatasetSummaryModel datasetSummaryModel, String resourcePath, String infix) throws Exception {

    SnapshotRequestModel snapshotRequest =
        jsonLoader.loadObject(resourcePath, SnapshotRequestModel.class);
    return createSnapshot(datasetSummaryModel, snapshotRequest, infix);
  }

  public SnapshotSummaryModel createSnapshot(
      DatasetSummaryModel datasetSummaryModel, SnapshotRequestModel snapshotRequest, String infix)
      throws Exception {

    MockHttpServletResponse response =
        launchCreateSnapshot(datasetSummaryModel, snapshotRequest, infix);

    return handleCreateSnapshotSuccessCase(response);
  }

  public MockHttpServletResponse launchCreateSnapshot(
      DatasetSummaryModel datasetSummaryModel, SnapshotRequestModel snapshotRequest, String infix)
      throws Exception {
    String snapshotName = Names.randomizeNameInfix(snapshotRequest.getName(), infix);

    return launchCreateSnapshotName(datasetSummaryModel, snapshotRequest, snapshotName);
  }

  public MockHttpServletResponse launchCreateSnapshotName(
      DatasetSummaryModel datasetSummaryModel,
      SnapshotRequestModel snapshotRequest,
      String snapshotName)
      throws Exception {

    snapshotRequest.getContents().get(0).setDatasetName(datasetSummaryModel.getName());
    snapshotRequest.profileId(datasetSummaryModel.getDefaultProfileId());
    snapshotRequest.setName(snapshotName);
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/snapshots")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(snapshotRequest)))
            .andReturn();

    return validateJobModelAndWait(result);
  }

  public SnapshotModel getSnapshot(UUID snapshotId) throws Exception {
    MvcResult result = mvc.perform(get("/api/repository/v1/snapshots/" + snapshotId)).andReturn();
    MockHttpServletResponse response = result.getResponse();
    return TestUtils.mapFromJson(response.getContentAsString(), SnapshotModel.class);
  }

  public void getSnapshotExpectError(UUID snapshotId, HttpStatus expectedStatus) throws Exception {
    MvcResult result = mvc.perform(get("/api/repository/v1/snapshots/" + snapshotId)).andReturn();
    handleFailureCase(result.getResponse(), expectedStatus);
  }

  public DatasetModel getDataset(UUID datasetId) throws Exception {
    MvcResult result = mvc.perform(get("/api/repository/v1/datasets/" + datasetId)).andReturn();
    return handleSuccessCase(result.getResponse(), DatasetModel.class);
  }

  public void getDatasetExpectError(UUID datasetId, HttpStatus expectedStatus) throws Exception {
    MvcResult result = mvc.perform(get("/api/repository/v1/datasets/" + datasetId)).andReturn();
    handleFailureCase(result.getResponse(), expectedStatus);
  }

  public SnapshotSummaryModel handleCreateSnapshotSuccessCase(MockHttpServletResponse response)
      throws Exception {
    SnapshotSummaryModel summaryModel = handleSuccessCase(response, SnapshotSummaryModel.class);
    addSnapshot(summaryModel.getId());
    return summaryModel;
  }

  public <T> T handleSuccessCase(MockHttpServletResponse response, Class<T> returnClass)
      throws Exception {
    String responseBody = response.getContentAsString();
    HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
    if (!responseStatus.is2xxSuccessful()) {
      String failMessage =
          "Request for " + returnClass.getName() + " failed: status=" + responseStatus;
      if (StringUtils.contains(responseBody, "message")) {
        // If the responseBody contains the word 'message', then we try to decode it as an
        // ErrorModel
        // so we can generate good failure information.
        ErrorModel errorModel = TestUtils.mapFromJson(responseBody, ErrorModel.class);
        failMessage += " msg=" + errorModel.getMessage();
      } else {
        failMessage += " responseBody=" + responseBody;
      }
      fail(failMessage);
    }

    return TestUtils.mapFromJson(responseBody, returnClass);
  }

  public ErrorModel handleFailureCase(MockHttpServletResponse response) throws Exception {
    return handleFailureCase(response, null);
  }

  public ErrorModel handleFailureCase(MockHttpServletResponse response, HttpStatus expectedStatus)
      throws Exception {
    HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());

    // check the failure status matches the expected
    // if no specific status is specified, just check that it's not successful
    if (expectedStatus == null) {
      assertFalse(responseStatus.is2xxSuccessful(), "Expect failure");
    } else {
      // assertThat("Expect specific failure status", responseStatus, is(expectedStatus));
      logger.info("expectedStatus=" + expectedStatus);
      logger.info("responseStatus=" + responseStatus);
      logger.info("responseBody=" + response.getContentAsString());
    }

    String responseBody = response.getContentAsString();
    assertThat("Error model was returned on failure", responseBody, containsString("message"));

    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
  }

  public void deleteTestDatasetAndCleanup(UUID id) throws Exception {
    deleteTestDataset(id);
    removeDatasetFromTracking(id);
  }

  public void deleteTestDataset(UUID id) throws Exception {
    MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + id)).andReturn();
    MockHttpServletResponse response = validateJobModelAndWait(result);
    checkDeleteResponse(response);
  }

  public void deleteTestProfile(UUID id) throws Exception {
    MvcResult result =
        mvc.perform(delete("/api/resources/v1/profiles/{id}?deleteCloudResources=true", id))
            .andReturn();
    MockHttpServletResponse response = validateJobModelAndWait(result);
    checkDeleteResponse(response);
  }

  public void deleteTestSnapshot(UUID id) throws Exception {
    MvcResult result = mvc.perform(delete("/api/repository/v1/snapshots/" + id)).andReturn();
    MockHttpServletResponse response = validateJobModelAndWait(result);
    checkDeleteResponse(response);
  }

  public void deleteTestFile(UUID datasetId, String fileId) throws Exception {
    MvcResult result =
        mvc.perform(delete("/api/repository/v1/datasets/" + datasetId + "/files/" + fileId))
            .andReturn();
    logger.info("deleting test file -  datasetId:{} objectId:{}", datasetId, fileId);
    MockHttpServletResponse response = validateJobModelAndWait(result);
    checkDeleteResponse(response);
  }

  public void deleteTestBucket(String bucketName) {
    storage.delete(bucketName);
  }

  public void deleteTestScratchFile(String path) {
    Blob scratchBlob = storage.get(BlobId.of(testConfig.getIngestbucket(), path));
    if (scratchBlob != null) {
      scratchBlob.delete();
    }
  }

  public void checkDeleteResponse(MockHttpServletResponse response) throws Exception {
    HttpStatus status = HttpStatus.valueOf(response.getStatus());
    if (status.is2xxSuccessful()) {
      DeleteResponseModel responseModel =
          TestUtils.mapFromJson(response.getContentAsString(), DeleteResponseModel.class);
      assertThat(
          "Valid delete response object state enumeration",
          responseModel.getObjectState(),
          is(
              oneOf(
                  DeleteResponseModel.ObjectStateEnum.DELETED,
                  DeleteResponseModel.ObjectStateEnum.NOT_FOUND)));
    } else {
      ErrorModel errorModel = handleFailureCase(response, HttpStatus.NOT_FOUND);
      assertThat("error model returned", errorModel, notNullValue());
    }
  }

  public MvcResult ingestTableRaw(
      UUID datasetId, IngestRequestModel ingestRequestModel, AuthenticatedUserRequest userReq)
      throws Exception {
    String jsonRequest = TestUtils.mapToJson(ingestRequestModel);
    String url = "/api/repository/v1/datasets/" + datasetId + "/ingest";

    return mvc.perform(
            performAs(post(url), userReq)
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
        .andReturn();
  }

  public IngestResponseModel ingestTableSuccess(
      UUID datasetId, IngestRequestModel ingestRequestModel) throws Exception {
    return ingestTableSuccess(datasetId, ingestRequestModel, null);
  }

  public IngestResponseModel ingestTableSuccess(
      UUID datasetId, IngestRequestModel ingestRequestModel, AuthenticatedUserRequest userRequest)
      throws Exception {
    MvcResult result = ingestTableRaw(datasetId, ingestRequestModel, userRequest);
    MockHttpServletResponse response = validateJobModelAndWait(result);

    return checkIngestTableResponse(response);
  }

  public void checkTableRowCount(
      FSContainerInterface tdrResource, String tableName, int expectedRowCount) {
    int rowCount = BigQueryPdao.getTableTotalRowCount(tdrResource, tableName);
    assertThat("Expected row count", rowCount, equalTo(expectedRowCount));
  }

  public void checkDataModel(
      FSContainerInterface tdrResource,
      List<String> columnNames,
      String tableName,
      int expectedRowCount)
      throws InterruptedException {
    List<BigQueryDataResultModel> results =
        BigQueryPdao.getTable(
            tdrResource,
            tableName,
            columnNames,
            expectedRowCount + 1,
            0,
            PDAO_ROW_ID_COLUMN,
            null,
            null);
    DataResultModel result = results.get(0);
    assertThat(
        "collection type should be defined as a snapshot or dataset.", tdrResource, notNullValue());
    switch (tdrResource.getCollectionType()) {
      case DATASET:
        assertThat(
            "Total row count should be correct since we includeTotalRowCount for datasets",
            result.getTotalCount(),
            equalTo(expectedRowCount));
        break;
      case SNAPSHOT:
        assertThat(
            "Total row count should be 0 since we do NOT includeTotalRowCount for snapshots",
            result.getTotalCount(),
            equalTo(0));
        break;
    }
    assertThat("Expected filtered count", result.getFilteredCount(), equalTo(expectedRowCount));
  }

  public IngestResponseModel checkIngestTableResponse(MockHttpServletResponse response)
      throws Exception {
    IngestResponseModel ingestResponse = handleSuccessCase(response, IngestResponseModel.class);
    assertThat("ingest response has no bad rows", ingestResponse.getBadRowCount(), equalTo(0L));

    return ingestResponse;
  }

  public void ingestTableFailure(UUID datasetId, IngestRequestModel ingestRequestModel)
      throws Exception {
    ingestTableFailure(datasetId, ingestRequestModel, null);
  }

  public void ingestTableFailure(
      UUID datasetId, IngestRequestModel ingestRequestModel, AuthenticatedUserRequest userRequest)
      throws Exception {
    MvcResult result = ingestTableRaw(datasetId, ingestRequestModel, userRequest);
    MockHttpServletResponse response = validateJobModelAndWait(result);
    handleFailureCase(response);
  }

  public FileModel ingestFileSuccess(UUID datasetId, FileLoadModel fileLoadModel) throws Exception {
    String jsonRequest = TestUtils.mapToJson(fileLoadModel);
    String url = "/api/repository/v1/datasets/" + datasetId + "/files";
    MvcResult result =
        mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(jsonRequest))
            .andReturn();

    MockHttpServletResponse response = validateJobModelAndWait(result);

    FileModel fileModel = handleSuccessCase(response, FileModel.class);
    checkSuccessfulFileLoad(fileLoadModel, fileModel, datasetId);

    return fileModel;
  }

  public enum RetryType {
    LOCK,
    UNLOCK
  }

  /*
   * Retry shared lock/unlock tests in FileOperationTest
   * Adjustable method to test acquiring locks during a file ingest while inserting different cases of exceptions:
   * Attempt to retry or fatal errors
   * Lock and unlock shared locks
   * Params:
   * retryType: Lock or unlock. If we're inserting an exception during the lock, then there won't be a shared lock.
   *            however, if we're inserting an exception during unlock, then we should have successfully acquired the
   *            shared lock
   * attemptRetry: If we don't attempt to retry after exception, then we expect the method to fail
   * removeFault: For retryable exceptions - if we never remove the fault, then we expect the method to fail
   * faultToInsert: the exception that we are inserting during the file ingest
   */
  public void retryAcquireLockIngestFileSuccess(
      RetryType retryType,
      boolean attemptRetry,
      boolean removeFault,
      ConfigEnum faultToInsert,
      UUID datasetId,
      FileLoadModel fileLoadModel,
      ConfigurationService configService,
      DatasetDao datasetDao)
      throws Exception {

    // setting the fault
    configService.setFault(faultToInsert.name(), true);

    String jsonRequest = TestUtils.mapToJson(fileLoadModel);
    String url = "/api/repository/v1/datasets/" + datasetId + "/files";
    MvcResult result =
        mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(jsonRequest))
            .andReturn();

    TimeUnit.SECONDS.sleep(5); // give the flight time to fail a couple of times
    DatasetDaoUtils datasetDaoUtils = new DatasetDaoUtils();
    String[] sharedLocks = datasetDaoUtils.getSharedLocks(datasetDao, datasetId);
    if (retryType == RetryType.LOCK) {
      assertThat("no shared locks after first call", sharedLocks.length, is(0));
    } else {
      assertThat("Acquire shared locks after first call", sharedLocks.length, is(1));
    }

    if (removeFault) {
      configService.setFault(faultToInsert.name(), false);
    }

    // get result
    MockHttpServletResponse response = validateJobModelAndWait(result);

    if (attemptRetry) {
      // make sure successful unlock
      TimeUnit.SECONDS.sleep(5);
      String[] sharedLocks3 = datasetDaoUtils.getSharedLocks(datasetDao, datasetId);
      assertThat("successful unlock", sharedLocks3.length, is(0));

      // Check if the flight successfully completed
      // Assume that if it successfully completed, then it was able to retry and acquire the shared
      // lock
      FileModel fileModel = handleSuccessCase(response, FileModel.class);
      checkSuccessfulFileLoad(fileLoadModel, fileModel, datasetId);
    } else {
      handleFailureCase(response);
      if (removeFault) {
        // Remove insertion of shared lock fault
        configService.setFault(faultToInsert.name(), false);
      }
    }
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  private void checkSuccessfulFileLoad(
      FileLoadModel fileLoadModel, FileModel fileModel, UUID datasetId) {
    assertThat(
        "description matches", fileModel.getDescription(), is(fileLoadModel.getDescription()));
    assertThat(
        "mime type matches",
        fileModel.getFileDetail().getMimeType(),
        is(fileLoadModel.getMimeType()));

    for (DRSChecksum checksum : fileModel.getChecksums()) {
      assertThat("valid checksum type", checksum.getType(), is(oneOf("crc32c", "md5")));
    }

    logger.info("addFile datasetId:{} objectId:{}", datasetId, fileModel.getFileId());
    addFile(datasetId.toString(), fileModel.getFileId());
  }

  public MvcResult softDeleteRaw(UUID datasetId, DataDeletionRequest softDeleteRequest)
      throws Exception {
    String softDeleteUrl = String.format("/api/repository/v1/datasets/%s/deletes", datasetId);
    return mvc.perform(
            post(softDeleteUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(softDeleteRequest)))
        .andReturn();
  }

  public void softDeleteSuccess(UUID datasetId, DataDeletionRequest softDeleteRequest)
      throws Exception {
    MvcResult result = softDeleteRaw(datasetId, softDeleteRequest);
    MockHttpServletResponse response = validateJobModelAndWait(result);
    handleSuccessCase(response, DeleteResponseModel.class);
  }

  public BulkLoadArrayResultModel ingestArraySuccess(
      UUID datasetId, BulkLoadArrayRequestModel loadModel) throws Exception {
    MvcResult result = ingestArrayRaw(datasetId, loadModel);
    MockHttpServletResponse response = validateJobModelAndWait(result);
    return handleSuccessCase(response, BulkLoadArrayResultModel.class);
  }

  public ErrorModel ingestArrayFailure(UUID datasetId, BulkLoadArrayRequestModel loadModel)
      throws Exception {
    MvcResult result = ingestArrayRaw(datasetId, loadModel);
    MockHttpServletResponse response = validateJobModelAndWait(result);
    return handleFailureCase(response);
  }

  public MvcResult ingestArrayRaw(UUID datasetId, BulkLoadArrayRequestModel loadModel)
      throws Exception {
    String jsonRequest = TestUtils.mapToJson(loadModel);
    String url = "/api/repository/v1/datasets/" + datasetId + "/files/bulk/array";
    return mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(jsonRequest))
        .andReturn();
  }

  public BulkLoadResultModel ingestBulkFileSuccess(UUID datasetId, BulkLoadRequestModel loadModel)
      throws Exception {
    MvcResult result = ingestBulkFileRaw(datasetId, loadModel);
    MockHttpServletResponse response = validateJobModelAndWait(result);
    return handleSuccessCase(response, BulkLoadResultModel.class);
  }

  public ErrorModel ingestBulkFileFailure(UUID datasetId, BulkLoadRequestModel loadModel)
      throws Exception {
    MvcResult result = ingestBulkFileRaw(datasetId, loadModel);
    MockHttpServletResponse response = validateJobModelAndWait(result);
    return handleFailureCase(response);
  }

  public MvcResult ingestBulkFileRaw(UUID datasetId, BulkLoadRequestModel loadModel)
      throws Exception {
    String jsonRequest = TestUtils.mapToJson(loadModel);
    String url = "/api/repository/v1/datasets/" + datasetId + "/files/bulk";
    return mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(jsonRequest))
        .andReturn();
  }

  public BulkLoadHistoryModelList getLoadHistory(
      UUID datasetId, String loadTag, int offset, int limit) throws Exception {
    var url = "/api/repository/v1/datasets/" + datasetId + "/files/bulk/" + loadTag;
    var result =
        mvc.perform(
                get(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .param("offset", Integer.toString(offset))
                    .param("limit", Integer.toString(limit)))
            .andReturn();

    return TestUtils.mapFromJson(
        result.getResponse().getContentAsString(), BulkLoadHistoryModelList.class);
  }

  public ErrorModel ingestFileFailure(UUID datasetId, FileLoadModel fileLoadModel)
      throws Exception {
    String jsonRequest = TestUtils.mapToJson(fileLoadModel);
    String url = "/api/repository/v1/datasets/" + datasetId + "/files";
    MvcResult result =
        mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(jsonRequest))
            .andReturn();

    MockHttpServletResponse response = validateJobModelAndWait(result);

    return handleFailureCase(response);
  }

  public MockHttpServletResponse lookupFileRaw(UUID datasetId, String fileId) throws Exception {
    String url = "/api/repository/v1/datasets/" + datasetId + "/files/" + fileId;
    MvcResult result = mvc.perform(get(url).contentType(MediaType.APPLICATION_JSON)).andReturn();
    return result.getResponse();
  }

  public MockHttpServletResponse lookupFileByPathRaw(UUID datasetId, String filePath, long depth)
      throws Exception {
    String url = "/api/repository/v1/datasets/" + datasetId + "/filesystem/objects";
    MvcResult result =
        mvc.perform(
                get(url)
                    .param("path", filePath)
                    .param("depth", Long.toString(depth))
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    return result.getResponse();
  }

  public MockHttpServletResponse lookupSnapshotFileRaw(UUID snapshotId, String objectId)
      throws Exception {
    String url = "/api/repository/v1/snapshots/" + snapshotId + "/files/" + objectId;
    MvcResult result = mvc.perform(get(url).contentType(MediaType.APPLICATION_JSON)).andReturn();
    return result.getResponse();
  }

  public FileModel lookupSnapshotFileSuccess(UUID snapshotId, String objectId) throws Exception {
    MockHttpServletResponse response = lookupSnapshotFileRaw(snapshotId, objectId);
    assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
    return TestUtils.mapFromJson(response.getContentAsString(), FileModel.class);
  }

  public MockHttpServletResponse lookupSnapshotFileByPathRaw(
      UUID snapshotId, String path, long depth) throws Exception {
    String url = "/api/repository/v1/snapshots/" + snapshotId + "/filesystem/objects";
    MvcResult result =
        mvc.perform(
                get(url)
                    .param("path", path)
                    .param("depth", Long.toString(depth))
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    return result.getResponse();
  }

  public FileModel lookupSnapshotFileByPathSuccess(UUID snapshotId, String path, long depth)
      throws Exception {
    MockHttpServletResponse response = lookupSnapshotFileByPathRaw(snapshotId, path, depth);
    assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
    return TestUtils.mapFromJson(response.getContentAsString(), FileModel.class);
  }

  public MockHttpServletResponse retrieveDatasetDataByIdRaw(
      UUID datasetId, String tableName, int limit, int offset, String filter, String sort)
      throws Exception {
    String url = "/api/repository/v1/datasets/{id}/data/{table}";
    var requestModel = new QueryDataRequestModel().limit(limit).offset(offset);
    if (sort != null) {
      requestModel.sort(sort);
    }
    if (filter != null) {
      requestModel.filter(filter);
    }
    MockHttpServletRequestBuilder request =
        post(url, datasetId, tableName)
            .contentType(MediaType.APPLICATION_JSON)
            .content(TestUtils.mapToJson(requestModel));
    MvcResult result = mvc.perform(request).andReturn();
    return result.getResponse();
  }

  public DatasetDataModel retrieveDatasetDataByIdSuccess(
      UUID datasetId, String tableName, int limit, int offset, String filter, String sort)
      throws Exception {
    MockHttpServletResponse response =
        retrieveDatasetDataByIdRaw(datasetId, tableName, limit, offset, filter, sort);
    assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
    return TestUtils.mapFromJson(response.getContentAsString(), DatasetDataModel.class);
  }

  public ErrorModel retrieveDatasetDataByIdFailure(
      UUID datasetId,
      String tableName,
      int limit,
      int offset,
      String filter,
      String sort,
      HttpStatus expectedStatus)
      throws Exception {
    MockHttpServletResponse response =
        retrieveDatasetDataByIdRaw(datasetId, tableName, limit, offset, filter, sort);
    return handleFailureCase(response, expectedStatus);
  }

  public MockHttpServletResponse retrieveSnapshotPreviewByIdRaw(
      UUID snapshotId, String tableName, int limit, int offset, String filter, String sort)
      throws Exception {
    String url = "/api/repository/v1/snapshots/{id}/data/{table}";
    var requestModel = new QueryDataRequestModel().limit(limit).offset(offset);
    if (sort != null) {
      requestModel.sort(sort);
    }
    if (filter != null) {
      requestModel.filter(filter);
    }
    MockHttpServletRequestBuilder request =
        post(url, snapshotId, tableName)
            .contentType(MediaType.APPLICATION_JSON)
            .content(TestUtils.mapToJson(requestModel));
    MvcResult result = mvc.perform(request).andReturn();
    return result.getResponse();
  }

  public enum TdrResourceType {
    SNAPSHOT,
    DATASET
  }

  public List<Object> retrieveDataSuccess(
      TdrResourceType resourceType,
      UUID resourceId,
      String tableName,
      int limit,
      int offset,
      String filter,
      String sort)
      throws Exception {
    return switch (resourceType) {
      case SNAPSHOT ->
          retrieveSnapshotPreviewByIdSuccess(resourceId, tableName, limit, offset, filter, sort)
              .getResult();
      case DATASET ->
          retrieveDatasetDataByIdSuccess(resourceId, tableName, limit, offset, filter, sort)
              .getResult();
    };
  }

  public ErrorModel retrieveDataFailure(
      TdrResourceType resourceType,
      UUID resourceId,
      String tableName,
      int limit,
      int offset,
      String filter,
      String sort,
      HttpStatus expectedStatus)
      throws Exception {
    return switch (resourceType) {
      case SNAPSHOT ->
          retrieveSnapshotPreviewByIdFailure(
              resourceId, tableName, limit, offset, filter, sort, expectedStatus);
      case DATASET ->
          retrieveDatasetDataByIdFailure(
              resourceId, tableName, limit, offset, filter, sort, expectedStatus);
    };
  }

  public SnapshotPreviewModel retrieveSnapshotPreviewByIdSuccess(
      UUID snapshotId, String tableName, int limit, int offset, String filter, String sort)
      throws Exception {
    MockHttpServletResponse response =
        retrieveSnapshotPreviewByIdRaw(snapshotId, tableName, limit, offset, filter, sort);
    assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
    return TestUtils.mapFromJson(response.getContentAsString(), SnapshotPreviewModel.class);
  }

  public ErrorModel retrieveSnapshotPreviewByIdFailure(
      UUID snapshotId,
      String tableName,
      int limit,
      int offset,
      String filter,
      String sort,
      HttpStatus expectedStatus)
      throws Exception {
    MockHttpServletResponse response =
        retrieveSnapshotPreviewByIdRaw(snapshotId, tableName, limit, offset, filter, sort);
    return handleFailureCase(response, expectedStatus);
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  public DRSObject drsGetObjectSuccess(String drsObjectId, boolean expand) throws Exception {
    String url = "/ga4gh/drs/v1/objects/" + drsObjectId;
    MvcResult result =
        mvc.perform(
                get(url)
                    .param("expand", Boolean.toString(expand))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();

    return TestUtils.mapFromJson(result.getResponse().getContentAsString(), DRSObject.class);
  }

  public void resetConfiguration() throws Exception {
    String url = "/api/repository/v1/configs/reset";
    mvc.perform(put(url).contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNoContent()) // HTTP status 204
        .andReturn();
  }

  public MockHttpServletResponse validateJobModelAndWait(MvcResult inResult) throws Exception {
    MvcResult result = inResult;
    while (true) {
      MockHttpServletResponse response = result.getResponse();
      HttpStatus status = HttpStatus.valueOf(response.getStatus());
      // When the status is not found, there is no job to poll
      if (status == HttpStatus.NOT_FOUND) {
        return result.getResponse();
      }
      assertThat(
          "expected jobs polling status, got " + status,
          status,
          is(oneOf(HttpStatus.ACCEPTED, HttpStatus.OK)));

      JobModel jobModel = TestUtils.mapFromJson(response.getContentAsString(), JobModel.class);
      String jobId = jobModel.getId();
      String locationUrl = response.getHeader("Location");
      assertThat("location URL was specified", locationUrl, notNullValue());

      switch (status) {
        case ACCEPTED:
          // Not done case: sleep and probe using the header URL
          assertThat(
              "location header for probe",
              locationUrl,
              equalTo(String.format("/api/repository/v1/jobs/%s", jobId)));

          TimeUnit.SECONDS.sleep(1);
          result = mvc.perform(get(locationUrl).accept(MediaType.APPLICATION_JSON)).andReturn();
          break;

        case OK:
          // Done case: get the result with the header URL and return the response;
          // let the caller interpret the response
          assertThat(
              "location header for result",
              locationUrl,
              equalTo(String.format("/api/repository/v1/jobs/%s/result", jobId)));
          result = mvc.perform(get(locationUrl).accept(MediaType.APPLICATION_JSON)).andReturn();
          return result.getResponse();

        default:
          fail("invalid response status");
      }
    }
  }

  // -- tracking methods --

  public void addDataset(UUID id) {
    logger.info(
        "Cleanup Tracking: Adding Dataset to list to be removed in cleanup. DatasetId: {}", id);
    createdDatasetIds.add(id);
  }

  public void removeDatasetFromTracking(UUID id) {
    logger.info("Cleanup Tracking: Removing Dataset from tracking list. DatasetId: {}", id);
    createdDatasetIds.remove(id);
  }

  public void addSnapshot(UUID id) {
    createdSnapshotIds.add(id);
  }

  public void addProfile(UUID id) {
    createdProfileIds.add(id);
  }

  public void addFile(String datasetId, String fileId) {
    String[] createdFile = {datasetId, fileId};
    createdFileIds.add(createdFile);
  }

  public void removeFile(UUID datasetId, String fileId) {
    String[] fileToRemove = null;
    for (String[] fileInfo : createdFileIds) {
      if (datasetId.toString().equals(fileInfo[0]) && fileId.equals(fileInfo[1])) {
        fileToRemove = fileInfo;
        break;
      }
    }
    if (fileToRemove != null) {
      createdFileIds.remove(fileToRemove);
    }
  }

  private MockHttpServletRequestBuilder performAs(
      MockHttpServletRequestBuilder requestBuilder, AuthenticatedUserRequest userReq) {
    if (userReq != null) {
      requestBuilder.header("From", userReq.getEmail());
    }
    return requestBuilder;
  }

  public void addBucket(String bucketName) {
    createdBuckets.add(bucketName);
  }

  // Scratch files are expected to be located in testConfig.getIngestBucket();
  public void addScratchFile(String path) {
    createdScratchFiles.add(path);
  }

  public void teardown() throws Exception {
    // call the reset configuration endpoint to disable all faults
    resetConfiguration();

    // Order is important: delete all the snapshots first so we eliminate dependencies
    // Then delete the files before the datasets
    for (UUID snapshotId : createdSnapshotIds) {
      try {
        deleteTestSnapshot(snapshotId);
      } catch (Exception ex) {
        logger.info("CLEANUP ERROR! Error deleting snapshot. SnapshotId: {}", snapshotId);
      }
    }

    for (String[] fileInfo : createdFileIds) {
      try {
        deleteTestFile(UUID.fromString(fileInfo[0]), fileInfo[1]);
      } catch (Exception ex) {
        logger.info("CLEANUP ERROR! Error deleting file. FileId: {}", fileInfo[0]);
      }
    }

    logger.info("Cleanup Tracking: {} datasets to be removed.", createdDatasetIds.size());
    for (UUID datasetId : createdDatasetIds) {
      logger.info("Cleanup Tracking: Dataset to be deleted {}", datasetId);
      try {
        deleteTestDataset(datasetId);
      } catch (Exception ex) {
        logger.info("CLEANUP ERROR! Error deleting dataset. DatasetId: {}", datasetId);
      }
    }

    for (UUID profileId : createdProfileIds) {
      try {
        deleteTestProfile(profileId);
      } catch (Exception ex) {
        logger.info("CLEANUP ERROR! Error deleting profile. ProfileId: {}", profileId);
      }
    }

    for (String bucketName : createdBuckets) {
      try {
        deleteTestBucket(bucketName);
      } catch (Exception ex) {
        logger.info("CLEANUP ERROR! Error deleting bucket. BucketName: {}", bucketName);
      }
    }

    for (String path : createdScratchFiles) {
      try {
        deleteTestScratchFile(path);
      } catch (Exception ex) {
        logger.info("CLEANUP ERROR! Error deleting scratch file. Path: {}", path);
      }
    }
  }

  public void deleteLoadHistory(UUID datasetId, TableServiceClient serviceClient) {
    var tableName = StorageTableName.LOAD_HISTORY.toTableName(datasetId);
    serviceClient.deleteTable(tableName);
  }
}
