package testscripts;

import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.*;
import io.kubernetes.client.openapi.models.*;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.KubernetesClientUtils;

public class DeletePod extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(KubeConfig.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public DeletePod() {
    super();
  }

  private int filesToLoad;
  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      filesToLoad = Integer.parseInt(parameters.get(0));
    }
  }

  public void setup(Map<String, ApiClient> apiClients) throws Exception {}

  // The purpose of this test is to have a long-running workload that completes successfully
  // while we delete pods and have them recover.
  public void userJourney(ApiClient apiClient) throws Exception {
    logger.debug("Deleteing pod");
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }
    KubernetesClientUtils.deleteRandomPod(apiDeployment);
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {}
}
