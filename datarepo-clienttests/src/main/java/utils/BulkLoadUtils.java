package utils;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkLoadUtils {
  private static final Logger logger = LoggerFactory.getLogger(BulkLoadUtils.class);

  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;
  private String loadTag;
  private AssertUtils assertUtils;

  public String getDatasetId() {
    if (datasetSummaryModel == null) {
      throw new UnsupportedOperationException("datasetSummaryModel has not been defined.");
    }
    return datasetSummaryModel.getId();
  }

  public void bulkLoadSetup(Map<String, ApiClient> apiClients, String billingAccount)
      throws Exception {
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

    logger.info("successfully created profile: " + billingProfileModel.getProfileName());

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi, billingProfileModel.getId(), "dataset-simple.json", true);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);

    logger.info("successfully created dataset: " + datasetSummaryModel.getName());
  }

  // This should run about 5 minutes on 2 DRmanager instances. The speed of loads is when
  // they are not local is about 2.5GB/minutes. With a fixed size of 1GB, each instance should do
  // 2.5 files per minute, so two instances should do 5 files per minute. To run 5 minutes we should
  // run 25 files.
  public BulkLoadArrayRequestModel buildBulkLoadFileRequest(int filesToLoad) {
    loadTag = FileUtils.randomizeName("longtest");

    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(billingProfileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(filesToLoad); // do not stop if there is a failure.

    logger.debug(
        "longFileLoadTest loading {} files into dataset id {}",
        filesToLoad,
        datasetSummaryModel.getId());

    // There are currently 26 source files, so if ingesting more files: continue to loop through the
    // source files,
    // but point to new target files file paths
    int numberOfSourceFiles = 26;
    for (int i = 0; i < filesToLoad; i++) {
      String fileBasePath = "/fileloadscaletest/file1GB-%02d.txt";
      String sourcePath =
          "gs://jade-testdata-uswestregion" + String.format(fileBasePath, i % numberOfSourceFiles);
      String targetPath = "/" + loadTag + String.format(fileBasePath, i);

      BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
      model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
      arrayLoad.addLoadArrayItem(model);
    }

    return arrayLoad;
  }

  public BulkLoadArrayRequestModel buildSecondArrayLoadTry() {
    return new BulkLoadArrayRequestModel().profileId(billingProfileModel.getId()).loadTag(loadTag);
  }

  public void getAndDisplayResults(RepositoryApi repositoryApi, JobModel bulkLoadArrayJobResponse)
      throws Exception {
    bulkLoadArrayJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, bulkLoadArrayJobResponse);

    BulkLoadArrayResultModel result =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, bulkLoadArrayJobResponse, BulkLoadArrayResultModel.class);
    
    BulkLoadResultModel loadSummary = result.getLoadSummary();
    assertUtils = new AssertUtils();
    assertUtils.assertEquals("Number of successful files loaded should equal total files.",
        loadSummary.getTotalFiles(), loadSummary.getSucceededFiles());
    logger.debug("Total files    : {}", loadSummary.getTotalFiles());
    logger.debug("Succeeded files: {}", loadSummary.getSucceededFiles());
    logger.debug("Failed files   : {}", loadSummary.getFailedFiles());
    logger.debug("Not Tried files: {}", loadSummary.getNotTriedFiles());
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    KubernetesClientUtils.changeReplicaSetSizeAndWait(1);
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

    logger.info("Successfully deleted dataset: {}", datasetSummaryModel.getName());

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());

    logger.info("Successfully deleted profile: {}", billingProfileModel.getProfileName());
  }
}
