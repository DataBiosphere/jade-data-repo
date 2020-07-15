package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.*;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Pod;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import utils.DataRepoUtils;
import utils.FileUtils;
import utils.KubernetesClientUtils;

public class KubeConfig extends runner.TestScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public KubeConfig() {
    super();
  }

  private int filesToLoad;
  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load between 1 and 25 in the parameters list");
    } else {
      // filesToLoad should be between 1 and 25. Default to 25.
      int filesToLoadParam = Integer.parseInt(parameters.get(0));
      filesToLoad = filesToLoadParam <= 25 && filesToLoadParam > 0 ? filesToLoadParam : 25;
    }
  }

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // create a new profile
    billingProfileModel =
        DataRepoUtils.createProfile(resourcesApi, billingAccount, "profile-simple", true);

    System.out.println("successfully created profile: " + billingProfileModel.getProfileName());

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi, billingProfileModel.getId(), "dataset-simple.json", true);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);

    System.out.println("successfully created dataset: " + datasetSummaryModel.getName());
  }

  // The purpose of this test is to have a long-running workload that completes successfully
  // while we delete pods and have them recover.
  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);
    // TODO - get namespace passed in
    V1Deployment deployment = KubernetesClientUtils.getApiDeployment();

    // set up and start bulk load job
    BulkLoadArrayRequestModel arrayLoad = buildBulkLoadFileRequest();
    JobModel bulkLoadArrayJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), arrayLoad);

    /*---------------------------------------------*/
    /* Manipulating kubernetes pods while scaling */
    bulkLoadArrayJobResponse =
        DataRepoUtils.pollForRunningJob(repositoryApi, bulkLoadArrayJobResponse, 30);
    if (bulkLoadArrayJobResponse.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
      System.out.println("Scaling pods down to 1");
      modifyKubernetesPostDeployment("sh", 1);
      bulkLoadArrayJobResponse =
          DataRepoUtils.pollForRunningJob(repositoryApi, bulkLoadArrayJobResponse, 30);
      if (bulkLoadArrayJobResponse.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
        System.out.println("Scaling pods back up to 4");
        modifyKubernetesPostDeployment("sh", 3);
      }
    }
    /*---------------------------------------------*/

    // wait for the job to complete and print out results
    getAndDisplayResults(repositoryApi, bulkLoadArrayJobResponse);
  }

  void modifyKubernetesPostDeployment(String namespace, int podCount) throws Exception {
    // set the initial number of pods in the API deployment replica set
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }
    System.out.println(
        "pod count before set initial replica set size: "
            + KubernetesClientUtils.listPodsForNamespace().size());
    apiDeployment = KubernetesClientUtils.changeReplicaSetSize(apiDeployment, podCount);
    KubernetesClientUtils.waitForReplicaSetSizeChange(apiDeployment, podCount);

    // print out the current pods
    List<V1Pod> pods = KubernetesClientUtils.listPodsForNamespace();
    System.out.println("initial number of pods: " + pods.size());
    for (V1Pod pod : pods) {
      System.out.println("  pod: " + pod.getMetadata().getName());
    }
  }

  // This should run about 5 minutes on 2 DRmanager instances. The speed of loads is when
  // they are not local is about 2.5GB/minutes. With a fixed size of 1GB, each instance should do
  // 2.5 files per minute, so two instances should do 5 files per minute. To run 5 minutes we should
  // run
  // 25 files. (There are 25 files in the directory, so if we need more we should do a reuse scheme
  // like the
  // fileLoadTest)
  private BulkLoadArrayRequestModel buildBulkLoadFileRequest() {
    String loadTag = FileUtils.randomizeName("longtest");

    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(billingProfileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(filesToLoad); // do not stop if there is a failure.

    System.out.println(
        "longFileLoadTest loading "
            + filesToLoad
            + " files into dataset id "
            + datasetSummaryModel.getId());

    for (int i = 0; i < filesToLoad; i++) {
      String tailPath = String.format("/fileloadscaletest/file1GB-%02d.txt", i);
      String sourcePath = "gs://jade-testdata-uswestregion" + tailPath;
      String targetPath = "/" + loadTag + tailPath;

      BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
      model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
      arrayLoad.addLoadArrayItem(model);
    }

    return arrayLoad;
  }

  private void getAndDisplayResults(RepositoryApi repositoryApi, JobModel bulkLoadArrayJobResponse)
      throws Exception {
    bulkLoadArrayJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, bulkLoadArrayJobResponse);
    BulkLoadArrayResultModel result =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, bulkLoadArrayJobResponse, BulkLoadArrayResultModel.class);

    BulkLoadResultModel loadSummary = result.getLoadSummary();
    System.out.println("Total files    : " + loadSummary.getTotalFiles());
    System.out.println("Succeeded files: " + loadSummary.getSucceededFiles());
    System.out.println("Failed files   : " + loadSummary.getFailedFiles());
    System.out.println("Not Tried files: " + loadSummary.getNotTriedFiles());
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
    deleteDatasetJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteDatasetJobResponse, DeleteResponseModel.class);

    System.out.println("successfully deleted dataset: " + datasetSummaryModel.getName());

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());

    System.out.println("successfully deleted profile: " + billingProfileModel.getProfileName());
  }
}
