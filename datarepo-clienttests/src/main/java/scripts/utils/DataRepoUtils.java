package scripts.utils;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.model.ConfigEnableModel;
import bio.terra.datarepo.model.ConfigGroupModel;
import bio.terra.datarepo.model.ConfigModel;
import bio.terra.datarepo.model.ConfigParameterModel;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.ErrorModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotRequestModel;
import bio.terra.datarepo.model.TableModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import common.utils.AuthenticationUtils;
import common.utils.FileUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;
import runner.config.TestUserSpecification;

public final class DataRepoUtils {
  private static final Logger logger = LoggerFactory.getLogger(DataRepoUtils.class);

  private DataRepoUtils() {}

  private static int maximumSecondsToWaitForJob = 500;
  private static int secondsIntervalToPollJob = 5;

  private static Map<TestUserSpecification, ApiClient> apiClientsForTestUsers = new HashMap<>();

  /**
   * Build the Data Repo API client object for the given test user and server specifications. This
   * class maintains a cache of API client objects, and will return the cached object if it already
   * exists. The token is always refreshed, regardless of whether the API client object was found in
   * the cache or not.
   *
   * @param testUser the test user whose credentials are supplied to the API client object
   * @param server the server we are testing against
   * @return the API client object for this user
   */
  public static ApiClient getClientForTestUser(
      TestUserSpecification testUser, ServerSpecification server) throws IOException {
    // refresh the user token
    GoogleCredentials userCredential = AuthenticationUtils.getDelegatedUserCredential(testUser);
    AccessToken userAccessToken = AuthenticationUtils.getAccessToken(userCredential);

    // first check if there is already a cached ApiClient for this test user
    ApiClient cachedApiClient = apiClientsForTestUsers.get(testUser);
    if (cachedApiClient != null) {
      // refresh the token here before returning
      // this should be helpful for long-running tests (roughly > 1hr)
      cachedApiClient.setAccessToken(userAccessToken.getTokenValue());

      return cachedApiClient;
    }

    // TODO: have ApiClients share an HTTP client, or one per each is ok?
    // no cached ApiClient found, so build a new one here and add it to the cache before returning
    logger.debug(
        "Fetching credentials and building Data Repo ApiClient object for test user: {}",
        testUser.name);
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(server.datarepoUri);

    apiClient.setAccessToken(userAccessToken.getTokenValue());

    apiClientsForTestUsers.put(testUser, apiClient);
    return apiClient;
  }

  // ====================================================================
  // General client utility methods

  /**
   * Wait until the job finishes, either successfully or not. Times out after {@link
   * DataRepoUtils#maximumSecondsToWaitForJob} seconds. Polls in intervals of {@link
   * DataRepoUtils#secondsIntervalToPollJob} seconds.
   *
   * @param repositoryApi the api object to use
   * @param job the job model to poll
   */
  public static JobModel waitForJobToFinish(RepositoryApi repositoryApi, JobModel job)
      throws Exception {
    logger.debug("Waiting for Data Repo job to finish");
    job = pollForRunningJob(repositoryApi, job, maximumSecondsToWaitForJob);

    if (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
      throw new RuntimeException(
          "Timed out waiting for job to finish. (jobid=" + job.getId() + ")");
    }

    return job;
  }

  /**
   * Poll for running job. Polls for designated time.
   *
   * @param repositoryApi the api object to use
   * @param job the job model to poll
   * @param pollTime time in seconds for the job to poll before returning
   */
  public static JobModel pollForRunningJob(RepositoryApi repositoryApi, JobModel job, int pollTime)
      throws Exception {
    int pollCtr = Math.floorDiv(pollTime, secondsIntervalToPollJob);
    job = repositoryApi.retrieveJob(job.getId());
    int tryCount = 1;

    while (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING) && pollCtr >= 0) {
      logger.debug("Sleeping. try #" + tryCount + " For Job: " + job.getDescription());
      TimeUnit.SECONDS.sleep(secondsIntervalToPollJob);
      job = repositoryApi.retrieveJob(job.getId());
      tryCount++;
      pollCtr--;
    }
    logger.debug("Status at end of polling: {}", job.getJobStatus());

    return job;
  }

  /**
   * Fetch the job result and de-serialize it to the specified result class. This method expects
   * that the job has already completed, either successfully or not.
   *
   * @param repositoryApi the api object to use
   * @param job the job model that has completed
   * @param resultClass the expected (model) class of the result
   * @return the de-serialized result
   */
  public static <T> T getJobResult(RepositoryApi repositoryApi, JobModel job, Class<T> resultClass)
      throws Exception {
    logger.debug("Fetching Data Repo job result");
    Object jobResult = repositoryApi.retrieveJobResult(job.getId());

    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.convertValue(jobResult, resultClass);
  }

  /**
   * Check the job status. If successful, fetch the job result and de-serialize it to the specified
   * result class. If failed, throw an exception that includes the error model. This method expects
   * that the job has already completed.
   *
   * @param repositoryApi the api object to use
   * @param jobResponse the job model that has completed
   * @param resultClass the expected (model) class of the result
   * @return the de-serialized result
   * @throws RuntimeException if the job status is failed
   */
  public static <T> T expectJobSuccess(
      RepositoryApi repositoryApi, JobModel jobResponse, Class<T> resultClass) throws Exception {

    if (jobResponse.getJobStatus().equals(JobModel.JobStatusEnum.FAILED)) {
      ErrorModel errorModel =
          DataRepoUtils.getJobResult(repositoryApi, jobResponse, ErrorModel.class);
      throw new RuntimeException("Job failed unexpectedly. " + errorModel);
    }

    return DataRepoUtils.getJobResult(repositoryApi, jobResponse, resultClass);
  }

  // ====================================================================
  // Endpoint-specific utility methods

  /**
   * Create a dataset and wait for the job to finish.
   *
   * @param repositoryApi the api object to use
   * @param profileId the billing profile id
   * @param apipayloadFilename the name of the create dataset payload file in the apipayloads
   *     resources directory
   * @param randomizeName true to append a random number at the end of the dataset name, false
   *     otherwise
   * @return the completed job model
   */
  public static JobModel createDataset(
      RepositoryApi repositoryApi,
      String profileId,
      String apipayloadFilename,
      boolean randomizeName)
      throws Exception {
    logger.debug("Creating a dataset");
    // use Jackson to map the stream contents to a DatasetRequestModel object
    ObjectMapper objectMapper = new ObjectMapper();
    InputStream datasetRequestFile =
        FileUtils.getResourceFileHandle("apipayloads/" + apipayloadFilename);
    DatasetRequestModel createDatasetRequest =
        objectMapper.readValue(datasetRequestFile, DatasetRequestModel.class);
    createDatasetRequest.defaultProfileId(profileId);

    if (randomizeName) {
      createDatasetRequest.setName(FileUtils.randomizeName(createDatasetRequest.getName()));
    }

    // make the create request and wait for the job to finish
    JobModel createDatasetJobResponse = repositoryApi.createDataset(createDatasetRequest);
    return DataRepoUtils.waitForJobToFinish(repositoryApi, createDatasetJobResponse);
  }

  /**
   * Create a snapshot and wait for the job to finish.
   *
   * @param repositoryApi the api object to use
   * @param datasetSummaryModel the summary of the dataset used by the snapshot
   * @param apipayloadFilename the name of the create snapshot payload file in the apipayloads
   *     resources directory
   * @param randomizeName true to append a random number at the end of the snapshot name, false
   *     otherwise
   * @return the completed job model
   */
  public static JobModel createSnapshot(
      RepositoryApi repositoryApi,
      DatasetSummaryModel datasetSummaryModel,
      String apipayloadFilename,
      boolean randomizeName)
      throws Exception {
    // use Jackson to map the stream contents to a SnapshotRequestModel object
    ObjectMapper objectMapper = new ObjectMapper();
    InputStream snapshotRequestFile =
        FileUtils.getResourceFileHandle("apipayloads/" + apipayloadFilename);
    SnapshotRequestModel createSnapshotRequest =
        objectMapper.readValue(snapshotRequestFile, SnapshotRequestModel.class);
    createSnapshotRequest.setProfileId(datasetSummaryModel.getDefaultProfileId());
    if (createSnapshotRequest.getContents().size() > 1) {
      throw new UnsupportedOperationException("This test requires content to be 1");
    }
    createSnapshotRequest.getContents().get(0).setDatasetName(datasetSummaryModel.getName());

    if (randomizeName) {
      createSnapshotRequest.setName(FileUtils.randomizeName(createSnapshotRequest.getName()));
    }

    // make the create request and wait for the job to finish
    JobModel createSnapshotJobResponse = repositoryApi.createSnapshot(createSnapshotRequest);
    return DataRepoUtils.waitForJobToFinish(repositoryApi, createSnapshotJobResponse);
  }

  /**
   * Create a billing profile.
   *
   * @param resourcesApi the api object to use
   * @param billingAccount the Google billing account id
   * @param profileName the name of the new profile
   * @param randomizeName true to append a random number at the end of the profile name, false
   *     otherwise
   * @return the created billing profile model
   */
  public static BillingProfileModel createProfile(
      ResourcesApi resourcesApi, String billingAccount, String profileName, boolean randomizeName)
      throws Exception {
    logger.debug("Creating a billing profile");

    if (randomizeName) {
      profileName = FileUtils.randomizeName(profileName);
    }

    BillingProfileRequestModel createProfileRequest =
        new BillingProfileRequestModel()
            .biller("direct")
            .billingAccountId(billingAccount)
            .profileName(profileName);

    // make the create request and wait for the job to finish
    BillingProfileModel createProfileResponse = resourcesApi.createProfile(createProfileRequest);
    return createProfileResponse;
  }

  public static void setConfigParameter(RepositoryApi repositoryApi, String name, int value)
      throws Exception {
    ConfigGroupModel groupModel =
        new ConfigGroupModel()
            .label("set parameter " + name)
            .addGroupItem(
                new ConfigModel()
                    .name(name)
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(new ConfigParameterModel().value(String.valueOf(value))));

    repositoryApi.setConfigList(groupModel);
  }

  /** Set a fault to enabled. */
  public static void enableFault(RepositoryApi repositoryApi, String faultName)
      throws ApiException {
    repositoryApi.setFault(faultName, new ConfigEnableModel().enabled(true));
  }

  /**
   * Build the name of a BigQuery table for a Data Repo dataset.
   *
   * @param datasetModel
   * @param tableModel
   * @return
   */
  public static String getBigQueryDatasetTableName(
      DatasetModel datasetModel, TableModel tableModel) {
    return datasetModel.getDataProject()
        + ".datarepo_"
        + datasetModel.getName()
        + "."
        + tableModel.getName();
  }
}
