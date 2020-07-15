package testscripts;

import bio.terra.datarepo.api.DataRepositoryServiceApi;
import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.*;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import utils.DataRepoUtils;
import utils.FileUtils;

public class RetrieveDRSSnapshot extends runner.TestScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public RetrieveDRSSnapshot() {
    super();
  }

  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;
  private SnapshotSummaryModel snapshotSummaryModel;

  private Storage storage;
  private List<String> createdScratchFiles;
  private String testConfigGetIngestbucket;

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator --- and the snapshot creator?
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    storage = StorageOptions.getDefaultInstance().getService();
    createdScratchFiles = new ArrayList<>();
    testConfigGetIngestbucket = "jade-testdata"; // this could be put in DRUtils

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

    // ingest a file
    URI sourceUri = new URI("gs://jade-testdata/fileloadprofiletest/1KBfile.txt");
    // todo generate a 1 byte file to load

    String targetPath = "/testrunner/IngestFile/" + FileUtils.randomizeName("") + ".txt";

    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("IngestFile")
            .mimeType("text/plain")
            .targetPath(targetPath)
            .profileId(datasetSummaryModel.getDefaultProfileId());
    JobModel ingestFileJobResponse =
        repositoryApi.ingestFile(datasetSummaryModel.getId(), fileLoadModel);
    ingestFileJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse);
    FileModel fileModel =
        DataRepoUtils.expectJobSuccess(repositoryApi, ingestFileJobResponse, FileModel.class);

    // generate a JSON file with the fileref
    String jsonLine =
        "{\"VCF_File_Name\":\"name1\", \"Description\":\"description1\", \"VCF_File_Ref\":\""
            + fileModel.getFileId()
            + "\"}\n";

    // load a JSON file that contains the table rows to load into the test bucket
    String jsonFileName = "this-better-pass.json";
    String dirInCloud = "scratch/testRetrieveSnapshot/" + UUID.randomUUID().toString();
    BlobInfo ingestTableBlob =
        BlobInfo.newBuilder(testConfigGetIngestbucket, dirInCloud + "/" + jsonFileName).build();

    storage.create(ingestTableBlob, jsonLine.getBytes(StandardCharsets.UTF_8));

    // save a reference to the JSON file so we can delete it in cleanup()
    createdScratchFiles.add(dirInCloud + "/" + jsonFileName);

    // ingest the tabular data from the JSON file we just generated
    String gsPath = "gs://" + testConfigGetIngestbucket + "/" + dirInCloud + "/" + jsonFileName;
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("vcf_file")
            .path(gsPath);
    JobModel ingestTabularDataJobResponse =
        repositoryApi.ingestDataset(datasetSummaryModel.getId(), ingestRequest);

    ingestTabularDataJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestTabularDataJobResponse);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);

    System.out.println("successfully loaded data into dataset: " + datasetSummaryModel.getName());

    // make the create snapshot request and wait for the job to finish
    JobModel createSnapshotJobResponse =
        DataRepoUtils.createSnapshot(
            repositoryApi, datasetSummaryModel, "snapshot-simple.json", true);

    // save a reference to the snapshot summary model so we can delete it in cleanup()
    snapshotSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);

    System.out.println("successfully created snapshot: " + snapshotSummaryModel.getName());
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);
    DataRepositoryServiceApi dataRepositoryServiceApi = new DataRepositoryServiceApi(apiClient);

    SnapshotModel snapshotModel = repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId());
    TableModel tableModel = snapshotModel.getTables().get(0);
    BigQuery bigQuery =
        BigQueryOptions.newBuilder()
            .setProjectId(snapshotModel.getDataProject())
            .build()
            .getService();
    // System.out.println(snapshotModel.getDataProject()); // broad-jade-mm-data
    // System.out.println(source.get(0).getDataset().getName()); //
    // DatasetSimple13213724609325394569
    // System.out.println(tableModel.getName()); // vcf_file
    String queryForFileRefs =
        "SELECT * FROM "
            + snapshotModel.getDataProject()
            + "."
            + snapshotModel.getName()
            + "."
            + tableModel.getName();

    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryForFileRefs).build();
    TimeUnit.SECONDS.sleep(60);
    TableResult result = bigQuery.query(queryConfig);

    ArrayList<String> fileRefs = new ArrayList<>();
    result.iterateAll().forEach(r -> fileRefs.add(r.get("VCF_File_Ref").getStringValue()));
    // fileRefs should only be 1 in size
    String fileModelFileId = fileRefs.get(0);
    System.out.println("well hello there");
    System.out.println(fileModelFileId); //

    String dirObjectId = "v1_" + snapshotSummaryModel.getId() + "_" + fileModelFileId;
    DRSObject object = dataRepositoryServiceApi.getObject(dirObjectId, false);
    System.out.println(dirObjectId);

    System.out.println(
        "successfully retrieved snaphot: "
            + object.getName()
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

    // delete scratch files -- This should be pulled into the test runner?
    for (String path : createdScratchFiles) {
      Blob scratchBlob = storage.get(BlobId.of(testConfigGetIngestbucket, path));
      if (scratchBlob != null) {
        scratchBlob.delete();
      }
    }

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());

    System.out.println("successfully deleted profile: " + billingProfileModel.getProfileName());
  }
}
