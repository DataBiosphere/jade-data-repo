package utils;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.ErrorModel;
import bio.terra.datarepo.model.JobModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public final class DataRepoUtils {

  private DataRepoUtils() {}

  private static int maximumSecondsToWaitForJob = 500;
  private static int secondsIntervalToPollJob = 5;

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
    int pollCtr = Math.floorDiv(maximumSecondsToWaitForJob, secondsIntervalToPollJob);
    job = repositoryApi.retrieveJob(job.getId());
    int tryCount = 1;

    while (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING) && pollCtr >= 0) {
      System.out.println("Sleeping. try #" + tryCount + " For Job: " + job.getDescription());
      TimeUnit.SECONDS.sleep(secondsIntervalToPollJob);
      job = repositoryApi.retrieveJob(job.getId());
      tryCount++;
      pollCtr--;
    }

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
   * @param pollTime time for the job to poll before returning
   */
  public static JobModel pollForRunningJob(RepositoryApi repositoryApi, JobModel job, int pollTime)
      throws Exception {
    int pollCtr = Math.floorDiv(pollTime, secondsIntervalToPollJob);
    job = repositoryApi.retrieveJob(job.getId());
    int tryCount = 1;

    while (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING) && pollCtr >= 0) {
      System.out.println("Sleeping. try #" + tryCount + "For Job: " + job.getDescription());
      // converting seconds to milliseconds
      Thread.sleep(secondsIntervalToPollJob * 1000);
      job = repositoryApi.retrieveJob(job.getId());
      tryCount++;
      pollCtr--;
    }

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
    // use Jackson to map the stream contents to a DatasetRequestModel object
    ObjectMapper objectMapper = new ObjectMapper();
    InputStream datasetRequestFile =
        FileUtils.getJSONFileHandle("apipayloads/" + apipayloadFilename);
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
}
