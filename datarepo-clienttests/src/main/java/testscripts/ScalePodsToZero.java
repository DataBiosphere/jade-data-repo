package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.BulkLoadUtils;
import utils.DataRepoUtils;
import utils.KubernetesClientUtils;

public class ScalePodsToZero extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(ScalePodsToZero.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public ScalePodsToZero() {
    super();
    manipulatesKubernetes = true; // this test script manipulates Kubernetes
  }

  private int filesToLoad;
  private BulkLoadUtils bulkLoadUtils;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      filesToLoad = Integer.parseInt(parameters.get(0));
    }
  }

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    bulkLoadUtils = new BulkLoadUtils();
    bulkLoadUtils.bulkLoadSetup(apiClients, billingAccount);
  }

  // The purpose of this test is to have a long-running workload that completes successfully
  // while we scale pods to zero and then scale them back up.
  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // set up and start bulk load job
    BulkLoadArrayRequestModel arrayLoad = bulkLoadUtils.buildBulkLoadFileRequest(filesToLoad);
    JobModel bulkLoadArrayJobResponse =
        repositoryApi.bulkFileLoadArray(bulkLoadUtils.getDatasetId(), arrayLoad);

    // =========================================================================
    /* Manipulating kubernetes pods during file ingest */

    // initial poll as file ingest begins
    bulkLoadArrayJobResponse =
        DataRepoUtils.pollForRunningJob(repositoryApi, bulkLoadArrayJobResponse, 30);

    if (bulkLoadArrayJobResponse.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
      logger.debug("Scaling pods down to 0");
      KubernetesClientUtils.changeReplicaSetSizeAndWait(0);

      try {
        bulkLoadArrayJobResponse =
            DataRepoUtils.pollForRunningJob(repositoryApi, bulkLoadArrayJobResponse, 30);
      } catch (ApiException ex) {
        logger.debug(
            "Catching expected exception while pod size = 0, Job Status: {}",
            bulkLoadArrayJobResponse.getJobStatus());
      }

      logger.debug("Scaling pods back up to 3.");
      KubernetesClientUtils.changeReplicaSetSizeAndWait(3);
      int retryCounter = 0;
      // give the job a few chances to get a non-failing results while the pods are scaled back up.
      ApiException lastException = null;
      while (bulkLoadArrayJobResponse.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)
          && retryCounter < 10) {
        retryCounter++;
        try {
          bulkLoadArrayJobResponse =
              DataRepoUtils.pollForRunningJob(repositoryApi, bulkLoadArrayJobResponse, 30);
          lastException = null;
        } catch (ApiException ex) {
          logger.debug(
              "Catching expected error while we wait for pods to scale back up. Retry # {}",
              retryCounter);
          lastException = ex;
          TimeUnit.SECONDS.sleep(30);
        }
      }
      if (lastException != null) {
        throw lastException;
      }
    }
    // =========================================================================

    // wait for the job to complete and print out results
    bulkLoadUtils.getAndDisplayResults(repositoryApi, bulkLoadArrayJobResponse);
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    KubernetesClientUtils.changeReplicaSetSizeAndWait(1);
    bulkLoadUtils.cleanup(apiClients);
  }
}
