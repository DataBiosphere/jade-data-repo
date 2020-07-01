package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.FileLoadModel;
import bio.terra.datarepo.model.FileModel;
import bio.terra.datarepo.model.JobModel;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import utils.DataRepoUtils;
import utils.FileUtils;

public class IngestFile extends runner.TestScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public IngestFile() {
    super();
  }

  private String datasetCreator;
  private DatasetSummaryModel datasetSummaryModel;

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the create request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(repositoryApi, "dataset-simple.json", true);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);

    System.out.println("successfully created dataset: " + datasetSummaryModel.getName());
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // TODO: parametrize this class to take the source file name/path. requires adding support for
    // passing constructor arguments through test config
    URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
    String targetPath = "/mm/IngestFile/" + FileUtils.randomizeName("1KBfile") + ".txt";

    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("IngestFile 1KB")
            .mimeType("text/plain")
            .targetPath(targetPath)
            .profileId(datasetSummaryModel.getDefaultProfileId());
    JobModel ingestFileJobResponse =
        repositoryApi.ingestFile(datasetSummaryModel.getId(), fileLoadModel);
    ingestFileJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse);
    FileModel fileModel =
        DataRepoUtils.expectJobSuccess(repositoryApi, ingestFileJobResponse, FileModel.class);

    System.out.println(
        "successfully ingested file: " + fileModel.getPath() + ", id: " + fileModel.getFileId());
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
    deleteDatasetJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteDatasetJobResponse, DeleteResponseModel.class);

    System.out.println("successfully deleted dataset: " + datasetSummaryModel.getName());
  }
}
