package scripts.testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.BulkLoadFileResultModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.google.cloud.storage.BlobId;
import common.utils.FileUtils;
import common.utils.StorageUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;

public class CreateSnapshot extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(CreateSnapshot.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public CreateSnapshot() {
    super();
  }

  private SnapshotModel snapshotModel;

  private static List<BlobId> scratchFiles = new ArrayList<>();

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // create the profile and dataset
    super.setup(testUsers);

    // get the ApiClient for the snapshot creator, same as the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // load data into the new dataset
    // note that there's a fileref in the dataset
    // ingest a file -- TODO CannedTestData.getMeA1KBFile
    URI sourceUri = new URI("gs://jade-testdata/fileloadprofiletest/1KBfile.txt");

    String loadTag = FileUtils.randomizeName("lookupTest");
    BulkLoadArrayRequestModel fileLoadModelArray =
        new BulkLoadArrayRequestModel()
            .profileId(datasetSummaryModel.getDefaultProfileId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);

    List<String> fileIds = new ArrayList<>();
    for (int i = 0; i < filesToLoad; i++) {
      String targetPath = "/testrunner/IngestFile/" + FileUtils.randomizeName("") + ".txt";

      BulkLoadFileModel fileLoadModel =
          new BulkLoadFileModel()
              .sourcePath(sourceUri.toString())
              .description("IngestFile")
              .mimeType("text/plain")
              .targetPath(targetPath);
      fileLoadModelArray.addLoadArrayItem(fileLoadModel);
      if (fileLoadModelArray.getLoadArray().size() % 1000 == 0 || i == filesToLoad - 1) {
        JobModel ingestFileJobResponse =
            repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), fileLoadModelArray);
        ingestFileJobResponse =
            DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse);
        BulkLoadArrayResultModel bulkLoadArrayResultModel =
            DataRepoUtils.expectJobSuccess(
                repositoryApi, ingestFileJobResponse, BulkLoadArrayResultModel.class);

        try (Stream<BulkLoadFileResultModel> resultStream =
            bulkLoadArrayResultModel.getLoadFileResults().stream()) {
          fileIds.addAll(
              resultStream.map(BulkLoadFileResultModel::getFileId).collect(Collectors.toList()));
        }
        fileLoadModelArray.getLoadArray().clear();
      }
    }
    String jsonLines;
    try (Stream<String> resultStream = fileIds.stream()) {
      // ingest the tabular data from the JSON file we just generated
      // generate a JSON file with the fileref
      jsonLines =
          resultStream
              .map(
                  fileId ->
                      "{\"VCF_File_Name\":\"name1\", \"Description\":\"description1\", \"VCF_File_Ref\":\""
                          + fileId
                          + "\"}")
              .collect(Collectors.joining("\n"));
    }
    byte[] fileRefBytes = jsonLines.getBytes(StandardCharsets.UTF_8);
    // load a JSON file that contains the table rows to load into the test bucket
    String jsonFileName = FileUtils.randomizeName("this-better-pass") + ".json";
    String fileRefName = "scratch/testDRSLookup/" + jsonFileName;

    String scratchFileBucketName = "jade-testdata";
    BlobId scratchFileTabularData =
        StorageUtils.writeBytesToFile(
            StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount),
            scratchFileBucketName,
            fileRefName,
            fileRefBytes);
    scratchFiles.add(scratchFileTabularData); // make sure the scratch file gets cleaned up later
    String gsPath = StorageUtils.blobIdToGSPath(scratchFileTabularData);

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
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());
  }

  private int filesToLoad;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      filesToLoad = Integer.parseInt(parameters.get(0));
    }
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    try {
      logger.info("Creating a snapshot");
      // get the ApiClient for the snapshot creator, same as the dataset creator
      ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(testUser, server);
      RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);
      // make the create snapshot request and wait for the job to finish
      JobModel createSnapshotJobResponse =
          DataRepoUtils.createSnapshot(
              repositoryApi, datasetSummaryModel, "snapshot-simple.json", true);

      logger.info("Snapshot job is done");
      if (createSnapshotJobResponse.getJobStatus() == JobModel.JobStatusEnum.FAILED) {
        throw new RuntimeException("Snapshot job did not finish successfully");
      }
      // save a reference to the snapshot summary model so we can delete it in cleanup()
      SnapshotSummaryModel snapshotSummaryModel =
          DataRepoUtils.expectJobSuccess(
              repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);
      logger.info("Successfully created snapshot: {}", snapshotSummaryModel.getName());

      // now go and retrieve the file Id that should be stored in the snapshot
      snapshotModel = repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId());
    } catch (Exception e) {
      logger.error("Error in journey", e);
      e.printStackTrace();
      throw e;
    }
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    logger.info("Tearing down");

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    if (snapshotModel != null) {
      JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotModel.getId());
      deleteSnapshotJobResponse =
          DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse);
      DataRepoUtils.expectJobSuccess(
          repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
      logger.info("Successfully deleted snapshot: {}", snapshotModel.getName());

      super.cleanup(testUsers);
      // delete the profile and dataset

      // delete the scratch files used for ingesting tabular data and soft delete rows
      StorageUtils.deleteFiles(
          StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);
    }
  }
}
