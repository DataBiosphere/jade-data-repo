package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;

import bio.terra.datarepo.model.*;
import com.google.api.client.util.Charsets;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.BigQueryUtils;
import utils.DataRepoUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.cloud.bigquery.TableResult;
import utils.FileUtils;

public class SoftDeleteDataset extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(SoftDeleteDataset.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public SoftDeleteDataset() {
    super();
  }

  private URI sourceFileURI;
  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;

  private Storage storage;
  private String testConfigGetIngestbucket;

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
    logger.debug("Source file URI: {}", sourceFileURI);
  }

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    storage = StorageOptions.getDefaultInstance().getService();
    testConfigGetIngestbucket = "jade-testdata"; // this could be put in DRUtils

    // create a new profile
    billingProfileModel =
        DataRepoUtils.createProfile(resourcesApi, billingAccount, "profile-simple", true);
    logger.info("Successfully created profile: {}", billingProfileModel.getProfileName());

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi, billingProfileModel.getId(), "dataset-simple.json", true);

/*
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
      ingestFileJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse);
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
      String fileRefName = "scratch/testRetrieveSnapshot/" + jsonFileName;
      String gsPath = FileUtils.createGsPath(fileRefBytes, fileRefName, testConfigGetIngestbucket);

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
      System.out.println("successfully loaded data into dataset: " + ingestResponse.getDataset()); */

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);
    logger.info("Successfully created dataset: {}", datasetSummaryModel.getName());
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // TODO where should I set PDAO constants?

    String datasetId = datasetSummaryModel.getId();
    DatasetModel datasetModel = repositoryApi.retrieveDataset(datasetId);
    String dataProject = datasetModel.getDataProject();

    // get row ids
        String participantTableRef = String.format("`%s.%s.%s`",
            dataProject,
            "datarepo_" + datasetModel.getName(),
            "participant");
        String participantSqlQuery = String.format("SELECT %s FROM %s LIMIT %s", "datarepo_row_id", participantTableRef, 3L);

        TableResult participantResult = BigQueryUtils.queryBigQuery(dataProject, participantSqlQuery);
        //assertThat("got right num of row ids back", sampleResult.getTotalRows(), equalTo(3L));
        List<String> participantRowIds = StreamSupport.stream(participantResult.getValues().spliterator(), false)
            .map(fieldValues -> fieldValues.get(0).getStringValue())
            .collect(Collectors.toList());

        String sampleTableRef = String.format("`%s.%s.%s`",
            dataProject,
            "datarepo_" + datasetModel.getName(),
            "sample");
        String sampleSqlQuery = String.format("SELECT %s FROM %s LIMIT %s", "datarepo_row_id", sampleTableRef, 2L);

        TableResult sampleResult = BigQueryUtils.queryBigQuery(dataProject, sampleSqlQuery);
        //assertThat("got right num of row ids back", sampleResult.getTotalRows(), equalTo(2L));
        List<String> sampleRowIds = StreamSupport.stream(sampleResult.getValues().spliterator(), false)
            .map(fieldValues -> fieldValues.get(0).getStringValue())
            .collect(Collectors.toList());

        // write them to GCS
        Storage storage = StorageOptions.getDefaultInstance().getService();
        String targetPath = "scratch/softDel/" + UUID.randomUUID().toString() + ".csv";
        BlobInfo participantBlob = BlobInfo.newBuilder(testConfigGetIngestbucket, targetPath).build();

        try (WriteChannel writer = storage.writer(participantBlob)) {
            for (String line : participantRowIds) {
                writer.write(ByteBuffer.wrap((line + "\n").getBytes(Charsets.UTF_8)));
            }
        }
        String participantPath =  String.format("gs://%s/%s", participantBlob.getBucket(), targetPath);

        BlobInfo sampleBlob = BlobInfo.newBuilder(testConfigGetIngestbucket, targetPath).build();
        try (WriteChannel writer = storage.writer(sampleBlob)) {
            for (String line : sampleRowIds) {
                writer.write(ByteBuffer.wrap((line + "\n").getBytes(Charsets.UTF_8)));
            }
        }
        String samplePath = String.format("gs://%s/%s", sampleBlob.getBucket(), targetPath);


      // build the deletion request with pointers to the two files with row ids to soft delete
      DataDeletionGcsFileModel deletionGcsParticipantFileModel = new DataDeletionGcsFileModel()
          .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
          .path(participantPath);
      DataDeletionTableModel participantDeletionTableFile = new DataDeletionTableModel()
          .tableName("participant")
          .gcsFileSpec(deletionGcsParticipantFileModel);

      DataDeletionGcsFileModel deletionGcsSampleFileModel = new DataDeletionGcsFileModel()
          .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
          .path(participantPath);
      DataDeletionTableModel sampleDeletionTableFile = new DataDeletionTableModel()
          .tableName("sample")
          .gcsFileSpec(deletionGcsSampleFileModel);

      List<DataDeletionTableModel> dataDeletionTableModels = Arrays.asList(
          participantDeletionTableFile,
          sampleDeletionTableFile);

      DataDeletionRequest dataDeletionRequest = new DataDeletionRequest()
          .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
          .specType(DataDeletionRequest.SpecTypeEnum.GCSFILE)
          .tables(dataDeletionTableModels);

      // send off the soft delete request
      JobModel softDeleteJobResponse = repositoryApi.applyDatasetDataDeletion(datasetId, dataDeletionRequest);
      softDeleteJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, softDeleteJobResponse);
      DeleteResponseModel deleteResponseModel = // TODO double check the response model
          DataRepoUtils.expectJobSuccess(repositoryApi, softDeleteJobResponse, DeleteResponseModel.class);

      // get row ids
      //List<String> participantRowIds = getRowIds(bigQuery, dataset, "participant", 3L);
      //List<String> sampleRowIds = getRowIds(bigQuery, dataset, "sample", 2L);

      // write them to GCS
      //String participantPath = writeListToScratch("softDel", participantRowIds);
      //String samplePath = writeListToScratch("softDel", sampleRowIds);

      // build the deletion request with pointers to the two files with row ids to soft delete
      //List<DataDeletionTableModel> dataDeletionTableModels = Arrays.asList(
        //  deletionTableFile("participant", participantPath),
        //  deletionTableFile("sample", samplePath));
      //DataDeletionRequest request = dataDeletionRequest()
        //  .tables(dataDeletionTableModels);

      // send off the soft delete request
      //dataRepoFixtures.deleteData(steward(), datasetId, request);

      // make sure the new counts make sense
      //assertTableCount(bigQuery, dataset, "participant", 2L);
      //assertTableCount(bigQuery, dataset, "sample", 5L);

      logger.info("Successfully soft deleted rows from dataset: {}", datasetSummaryModel.getName());
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
    logger.info("Successfully deleted dataset: {}", datasetSummaryModel.getName());

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());
    logger.info("Successfully deleted profile: {}", billingProfileModel.getProfileName());
  }
}
