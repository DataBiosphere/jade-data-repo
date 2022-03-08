package scripts.testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DataDeletionGcsFileModel;
import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.model.DataDeletionTableModel;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import com.google.api.client.util.Charsets;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobId;
import common.utils.BigQueryUtils;
import common.utils.FileUtils;
import common.utils.StorageUtils;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.BulkLoadUtils;
import scripts.utils.DataRepoUtils;

public class SoftDeleteDataset extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(SoftDeleteDataset.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public SoftDeleteDataset() {
    super();
  }

  private DataDeletionRequest dataDeletionRequest;
  private static List<BlobId> scratchFiles = new ArrayList<>();

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // create the profile and dataset
    super.setup(testUsers);

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // load data into the new dataset
    // note that there's a fileref in the dataset
    // ingest a file -- TODO CannedTestData.getMeA1KBFile
    URI sourceUri = new URI("gs://jade-testdata/fileloadprofiletest/1KBfile.txt");

    String targetPath = "/testrunner/softDel/" + FileUtils.randomizeName("") + ".txt";

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .sourcePath(sourceUri.toString())
            .description("softDel")
            .mimeType("text/plain")
            .targetPath(targetPath);
    String loadTag = FileUtils.randomizeName("softDelTest");
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

    String jsonFileName = FileUtils.randomizeName("this-better-pass") + ".json";
    String dirInCloud = "scratch/softDel";
    String fileRefName = dirInCloud + "/" + jsonFileName;

    String scratchFileBucketName = "jade-testdata";
    BlobId scratchFileTabularData =
        BulkLoadUtils.writeScratchFileForIngestRequest(
            server.testRunnerServiceAccount,
            bulkLoadArrayResultModel,
            scratchFileBucketName,
            fileRefName);
    IngestRequestModel ingestRequest =
        BulkLoadUtils.makeIngestRequestFromScratchFile(scratchFileTabularData);
    scratchFiles.add(scratchFileTabularData); // make sure the scratch file gets cleaned up later

    JobModel ingestTabularDataJobResponse =
        repositoryApi.ingestDataset(datasetSummaryModel.getId(), ingestRequest);

    ingestTabularDataJobResponse =
        DataRepoUtils.waitForJobToFinish(
            repositoryApi, ingestTabularDataJobResponse, datasetCreator);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());

    UUID datasetId = datasetSummaryModel.getId();
    DatasetModel datasetModel = repositoryApi.retrieveDataset(datasetId, Collections.emptyList());
    String dataProject = datasetModel.getDataProject();
    String tableName = datasetModel.getSchema().getTables().get(0).getName();

    // get row ids for table
    String sqlQuery =
        BigQueryUtils.buildSelectQuery(
            dataProject,
            BigQueryUtils.getDatasetName(datasetModel.getName()),
            tableName,
            "datarepo_row_id",
            1L);
    BigQuery bigQueryClient =
        BigQueryUtils.getClientForTestUser(datasetCreator, datasetModel.getDataProject());
    TableResult result = BigQueryUtils.queryBigQuery(bigQueryClient, sqlQuery);
    List<String> rowIds =
        StreamSupport.stream(result.getValues().spliterator(), false)
            .map(fieldValues -> fieldValues.get(0).getStringValue())
            .collect(Collectors.toList());

    logger.info("Successfully retrieved row ids for table: {}", tableName);

    // write to GCS
    String csvFileName = FileUtils.randomizeName("this-too-better-pass") + ".csv";
    String csvFileRefName = dirInCloud + "/" + csvFileName;

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (String line : rowIds) {
      output.write((line + "\n").getBytes(Charsets.UTF_8));
    }
    byte[] bytes = output.toByteArray();

    BlobId scratchFileSoftDeleteRows =
        StorageUtils.writeBytesToFile(
            StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount),
            scratchFileBucketName,
            csvFileRefName,
            bytes);
    scratchFiles.add(scratchFileSoftDeleteRows); // make sure the scratch file gets cleaned up later
    String gcsPath = StorageUtils.blobIdToGSPath(scratchFileSoftDeleteRows);

    // build the deletion request with pointers to the file with row ids to soft delete
    DataDeletionGcsFileModel deletionGcsFileModel =
        new DataDeletionGcsFileModel()
            .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
            .path(gcsPath);
    DataDeletionTableModel deletionTableFile =
        new DataDeletionTableModel().tableName(tableName).gcsFileSpec(deletionGcsFileModel);
    List<DataDeletionTableModel> dataDeletionTableModels =
        Collections.singletonList(deletionTableFile);
    dataDeletionRequest =
        new DataDeletionRequest()
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .specType(DataDeletionRequest.SpecTypeEnum.GCSFILE)
            .tables(dataDeletionTableModels);
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // send off the soft delete request
    JobModel softDeleteJobResponse =
        repositoryApi.applyDatasetDataDeletion(datasetSummaryModel.getId(), dataDeletionRequest);
    softDeleteJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, softDeleteJobResponse, datasetCreator);
    DeleteResponseModel deleteResponseModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, softDeleteJobResponse, DeleteResponseModel.class);
    logger.debug(
        "Successfully soft deleted rows from dataset: {} with state {}",
        datasetSummaryModel.getName(),
        deleteResponseModel.getObjectState());
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    // delete the profile and dataset
    super.cleanup(testUsers);

    // delete the scratch files used for ingesting tabular data and soft delete rows
    StorageUtils.deleteFiles(
        StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);
  }
}
