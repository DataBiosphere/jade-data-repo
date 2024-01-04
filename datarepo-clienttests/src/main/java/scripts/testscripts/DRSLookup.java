package scripts.testscripts;

import bio.terra.datarepo.api.DataRepositoryServiceApi;
import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DRSObject;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import bio.terra.datarepo.model.TableModel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobId;
import common.utils.BigQueryUtils;
import common.utils.FileUtils;
import common.utils.StorageUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;

/*
 * WARNING: if making any changes to this class make sure to notify the #dsp-batch channel! Describe the change and
 * any consequences downstream to DRS clients.
 */
public class DRSLookup extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(DRSLookup.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public DRSLookup() {
    super();
  }

  private SnapshotModel snapshotModel;

  private static List<BlobId> scratchFiles = new ArrayList<>();
  private String dirObjectId;

  private int NUM_DRS_LOOKUPS = 1;

  public void setParameters(List<String> parameters) {
    if (parameters != null && parameters.size() > 0) {
      NUM_DRS_LOOKUPS = Integer.parseInt(parameters.get(0));
    }
    logger.debug("Repeated DRS Lookups (default is 1): {}", NUM_DRS_LOOKUPS);
  }

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

    String targetPath = "/testrunner/IngestFile/" + FileUtils.randomizeName("") + ".txt";

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .sourcePath(sourceUri.toString())
            .description("IngestFile")
            .mimeType("text/plain")
            .targetPath(targetPath);
    String loadTag = FileUtils.randomizeName("lookupTest");
    BulkLoadArrayRequestModel fileLoadModelArray =
        new BulkLoadArrayRequestModel()
            .profileId(datasetSummaryModel.getDefaultProfileId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);
    fileLoadModelArray.addLoadArrayItem(fileLoadModel);

    JobModel ingestFileJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), fileLoadModelArray);
    ingestFileJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse, datasetCreator);
    BulkLoadArrayResultModel bulkLoadArrayResultModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestFileJobResponse, BulkLoadArrayResultModel.class);
    String fileId = bulkLoadArrayResultModel.getLoadFileResults().get(0).getFileId();

    // ingest the tabular data from the JSON file we just generated
    // generate a JSON file with the fileref
    String jsonLine =
        "{\"VCF_File_Name\":\"name1\", \"Description\":\"description1\", \"VCF_File_Ref\":\""
            + fileId
            + "\"}\n";
    byte[] fileRefBytes = jsonLine.getBytes(StandardCharsets.UTF_8);
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
        DataRepoUtils.waitForJobToFinish(
            repositoryApi, ingestTabularDataJobResponse, datasetCreator);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());

    // make the create snapshot request and wait for the job to finish
    JobModel createSnapshotJobResponse =
        DataRepoUtils.createSnapshot(
            repositoryApi, datasetSummaryModel, "snapshot-simple.json", datasetCreator, true);

    // save a reference to the snapshot summary model so we can delete it in cleanup()
    SnapshotSummaryModel snapshotSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);
    logger.info("Successfully created snapshot: {}", snapshotSummaryModel.getName());

    for (TestUserSpecification user : testUsers) {
      if (!StringUtils.equals(datasetCreator.userEmail, user.userEmail)) {
        repositoryApi.addSnapshotPolicyMember(
            snapshotSummaryModel.getId(),
            "steward",
            new PolicyMemberRequest().email(user.userEmail));
        logger.info(
            "Granted steward access on snapshot {} to user {}",
            snapshotSummaryModel.getName(),
            user.userEmail);
      }
    }

    // now go and retrieve the file Id that should be stored in the snapshot
    snapshotModel =
        repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId(), Collections.emptyList());

    TableModel tableModel =
        snapshotModel.getTables().get(0); // There is only 1 table, so just grab the first

    String queryForFileRefs =
        BigQueryUtils.buildSelectQuery(
            snapshotModel.getDataProject(),
            snapshotModel.getName(),
            tableModel.getName(),
            "VCF_File_Ref",
            1L);

    BigQuery bigQueryClient =
        BigQueryUtils.getClientForTestUser(datasetCreator, snapshotModel.getDataProject());
    TableResult result = BigQueryUtils.queryBigQuery(bigQueryClient, queryForFileRefs);
    ArrayList<String> fileRefs = new ArrayList<>();
    result.iterateAll().forEach(r -> fileRefs.add(r.get("VCF_File_Ref").getStringValue()));
    // fileRefs should only be 1 in size
    logger.info("Successfully retrieved file refs: {}", fileRefs);
    String fileModelFileId = fileRefs.get(0);
    String freshFileId = fileModelFileId.split("_")[2];
    dirObjectId = "v1_" + snapshotSummaryModel.getId() + "_" + freshFileId;
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    DataRepositoryServiceApi dataRepositoryServiceApi = new DataRepositoryServiceApi(apiClient);
    for (int i = 0; i < NUM_DRS_LOOKUPS; i++) {
      DRSObject object = dataRepositoryServiceApi.getObject(dirObjectId, false);
      logger.debug(
          "Successfully retrieved drs object: {}, with id: {} and data project: {}",
          object.getName(),
          dirObjectId,
          snapshotModel.getDataProject());
    }
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotModel.getId());
    deleteSnapshotJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse, datasetCreator);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
    logger.info("Successfully deleted snapshot: {}", snapshotModel.getName());

    // delete the profile and dataset
    super.cleanup(testUsers);

    // delete the scratch files used for ingesting tabular data and soft delete rows
    StorageUtils.deleteFiles(
        StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);
  }
}
