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

public class KillKubePods extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(KillKubePods.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public KillKubePods() {
    super();
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
  // while we delete pods and have them recover.
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
      logger.info("Scaling pods down to 0");
      KubernetesClientUtils.changeReplicaSetSizeAndWait(0);

      try {
        bulkLoadArrayJobResponse =
            DataRepoUtils.pollForRunningJob(repositoryApi, bulkLoadArrayJobResponse, 30);
      } catch (ApiException ex) {
        logger.info("Catching expected exception while pod size = 0");
      }

      logger.info("Scaling pods back up to 1.");
      KubernetesClientUtils.changeReplicaSetSizeAndWait(1);
      // give the poll a few changes to get a non-failing results while the pods are scaled back up.
      ApiException lastException = null;
      for (int i = 0; i < 5; i++) {
        try {
          bulkLoadArrayJobResponse =
              DataRepoUtils.pollForRunningJob(repositoryApi, bulkLoadArrayJobResponse, 30);
          lastException = null;
        } catch (ApiException ex) {
          logger.info("Catching errors while we wait for pod to come back up. Retry # {}");
          lastException = ex;
          TimeUnit.SECONDS.sleep(15);
        }
      }
      if (lastException != null) {
        throw lastException;
      }

      // if job still running, scale back up
      /*if (bulkLoadArrayJobResponse.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
        logger.debug("Scaling pods back up to 4");
        KubernetesClientUtils.changeReplicaSetSizeAndWait(4);
      }*/
    }
    // =========================================================================

    // wait for the job to complete and print out results
    bulkLoadUtils.getAndDisplayResults(repositoryApi, bulkLoadArrayJobResponse);
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    bulkLoadUtils.cleanup(apiClients);
  }
}
