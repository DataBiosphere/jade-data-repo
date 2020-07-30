package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.JobModel;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.BulkLoadUtils;
import utils.DataRepoUtils;
import utils.KubernetesClientUtils;

public class PodDelete extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(ScalePodsToZero.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public PodDelete() {
    super();
    manipulatesKubernetes = true; // this test script manipulates Kubernetess
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
  // while we delete a random pod.
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
      KubernetesClientUtils.deleteRandomPod();
    } else {
      throw new Exception("Job finished before we were able to test the delete functionality.");
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
