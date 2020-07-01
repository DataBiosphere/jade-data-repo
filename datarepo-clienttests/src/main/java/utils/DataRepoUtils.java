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

public final class DataRepoUtils {

  private DataRepoUtils() {}

  private static int maximumSecondsToWaitForJob = 500;
  private static int secondsIntervalToPollJob = 5;

  // ====================================================================
  // General client utility methods

  public static JobModel waitForJobToFinish(RepositoryApi repositoryApi, JobModel job)
      throws Exception {
    int pollCtr = Math.floorDiv(maximumSecondsToWaitForJob, secondsIntervalToPollJob);
    job = repositoryApi.retrieveJob(job.getId());

    while (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING) && pollCtr >= 0) {
      job = repositoryApi.retrieveJob(job.getId());
      pollCtr--;
      Thread.sleep(secondsIntervalToPollJob);
    }

    if (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
      throw new RuntimeException(
          "Timed out waiting for job to finish. (jobid=" + job.getId() + ")");
    }

    return job;
  }

  public static <T> T getJobResult(RepositoryApi repositoryApi, JobModel job, Class<T> resultClass)
      throws Exception {
    Object jobResult = repositoryApi.retrieveJobResult(job.getId());

    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.convertValue(jobResult, resultClass);
  }

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
