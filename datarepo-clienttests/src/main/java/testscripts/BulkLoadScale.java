package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.*;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.BulkLoadUtils;
import utils.KubernetesClientUtils;

public class BulkLoadScale extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(BulkLoadScale.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public BulkLoadScale() {
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

  // The purpose of this test is to measure scaling of bulk load.
  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // set up and start bulk load job
    BulkLoadArrayRequestModel arrayLoad = bulkLoadUtils.buildBulkLoadFileRequest(filesToLoad);
    JobModel bulkLoadArrayJobResponse =
        repositoryApi.bulkFileLoadArray(bulkLoadUtils.getDatasetId(), arrayLoad);

    // wait for the job to complete and print out results
    bulkLoadUtils.getAndDisplayResults(repositoryApi, bulkLoadArrayJobResponse);
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
      KubernetesClientUtils.changeReplicaSetSizeAndWait(1);
      bulkLoadUtils.cleanup(apiClients);
  }
}
