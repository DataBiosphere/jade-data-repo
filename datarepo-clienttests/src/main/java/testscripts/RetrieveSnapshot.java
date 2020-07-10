package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import utils.DataRepoUtils;

public class RetrieveSnapshot extends runner.TestScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public RetrieveSnapshot() {
    super();
  }

  private URI sourceFileURI;
  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;
  private SnapshotSummaryModel snapshotSummaryModel;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a URI for the source file in the parameters list");
    } else
      try {
        sourceFileURI = new URI(parameters.get(0));
      } catch (URISyntaxException synEx) {
        throw new RuntimeException("Error parsing source file URI: " + parameters.get(0), synEx);
      }
    System.out.println("source file URI: " + sourceFileURI);
  }

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator --- and the snapshot creator?
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

    // load data into the new dataset
    // note that there's a fileref in the dataset
    // TODO GOOD LORD THIS IS WRONG

    // ingest a file
    /*URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
        String targetFilePath =
            "/mm/" + Names.randomizeName("testdir") + "/testExcludeLockedFromSnapshotFileLookups.txt";
        FileLoadModel fileLoadModel =
            new FileLoadModel()
                .sourcePath(sourceUri.toString())
                .description("testExcludeLockedFromSnapshotFileLookups")
                .mimeType("text/plain")
                .targetPath(targetFilePath)
                .profileId(billingProfile.getId());
        FileModel fileModel =
            connectedOperations.ingestFileSuccess(datasetRefSummary.getId(), fileLoadModel);

        // generate a JSON file with the fileref
        String jsonLine = "{\"name\":\"name1\", \"file_ref\":\"" + fileModel.getFileId() + "\"}\n";

        // load a JSON file that contains the table rows to load into the test bucket
        String jsonFileName = "this-better-pass.json";
        String dirInCloud =
            "scratch/testExcludeLockedFromSnapshotFileLookups/" + UUID.randomUUID().toString();
        BlobInfo ingestTableBlob =
            BlobInfo.newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + jsonFileName).build();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        storage.create(ingestTableBlob, jsonLine.getBytes(StandardCharsets.UTF_8));

        // make sure the JSON file gets cleaned up on test teardown
        connectedOperations.addScratchFile(dirInCloud + "/" + jsonFileName);

        // ingest the tabular data from the JSON file we just generated
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + jsonFileName;
        IngestRequestModel ingestRequest1 =
            new IngestRequestModel()
                .format(IngestRequestModel.FormatEnum.JSON)
                .table("tableA")
                .path(gsPath);
        connectedOperations.ingestTableSuccess(datasetRefSummary.getId(), ingestRequest1);
    */
    System.out.println("successfully loaded data into dataset: " + datasetSummaryModel.getName());

    // make the create snapshot request and wait for the job to finish
    JobModel createSnapshotJobResponse =
        DataRepoUtils.createSnapshot(
            repositoryApi, billingProfileModel.getId(), "snapshot-simple.json", true);

    // save a reference to the snapshot summary model so we can delete it in cleanup()
    snapshotSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);

    System.out.println("successfully created snapshot: " + snapshotSummaryModel.getName());
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    SnapshotModel snapshotModel = repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId());

    System.out.println(
        "successfully retrieved snaphot: "
            + snapshotModel.getName()
            + ", data project: "
            + snapshotModel.getDataProject());
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotSummaryModel.getId());
    deleteSnapshotJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);

    // make the delete request and wait for the job to finish
    JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
    deleteDatasetJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteDatasetJobResponse, DeleteResponseModel.class);

    System.out.println("successfully deleted snapshot: " + snapshotSummaryModel.getName());
    System.out.println("successfully deleted dataset: " + datasetSummaryModel.getName());

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());

    System.out.println("successfully deleted profile: " + billingProfileModel.getProfileName());
  }
}
