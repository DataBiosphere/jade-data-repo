package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import bio.terra.common.GcsUtils;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.SamFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSObject;
import bio.terra.model.DataDeletionJsonArrayModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.StorageRoles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

// TODO move me to integration dir
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class SelfHostedDatasetIntegrationTest extends UsersBase {
  private static Logger logger = LoggerFactory.getLogger(SelfHostedDatasetIntegrationTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private AuthService authService;
  @Autowired private GcsUtils gcsUtils;
  @Autowired private SamFixtures samFixtures;
  @Autowired private GoogleResourceManagerService resourceManagerService;

  @Rule @Autowired public TestJobWatcher testWatcher;

  private String stewardToken;
  private UUID datasetId;
  private UUID snapshotId;
  private String snapshotProject;
  private UUID profileId;
  private List<String> uploadedFiles;
  private String ingestServiceAccount;
  private String ingestBucket;
  private Map<String, String> directFileIngestTargetPathsToIds;

  @Before
  public void setup() throws Exception {
    logger.info("Beginning test setup...");
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    uploadedFiles = new ArrayList<>();
    directFileIngestTargetPathsToIds = new HashMap<>();
    logger.info("Test setup complete.");
  }

  @After
  public void teardown() throws Exception {
    logger.info("Beginning test teardown...");
    dataRepoFixtures.resetConfig(steward());

    if (ingestServiceAccount != null && ingestBucket != null) {
      DatasetIntegrationTest.removeServiceAccountRoleFromBucket(
          ingestBucket, ingestServiceAccount, StorageRoles.objectViewer(), snapshotProject);

      DatasetIntegrationTest.removeServiceAccountRoleFromBucket(
          ingestBucket, ingestServiceAccount, StorageRoles.legacyBucketReader(), snapshotProject);
    }

    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    }

    if (ingestServiceAccount != null) {
      samFixtures.deleteServiceAccountFromTerra(steward(), ingestServiceAccount);
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }

    for (var path : uploadedFiles) {
      gcsUtils.deleteTestFile(path);
    }
    logger.info("Test teardown complete.");
  }

  @Test
  public void testSelfHostedDatasetLifecycle() throws Exception {
    testSelfHostedDatasetLifecycle("jade-testdata-useastregion", false);
  }

  @Test
  public void testSelfHostedDatasetWithDedicatedSALifecycle() throws Exception {
    testSelfHostedDatasetLifecycle("jade_testbucket_no_jade_sa", true);
  }

  @Test
  public void testSelfHostedDatasetRequesterPaysLifecycle() throws Exception {
    testSelfHostedDatasetLifecycle("jade_testbucket_requester_pays", true);
  }

  private void testSelfHostedDatasetLifecycle(String ingestBucket, boolean dedicatedServiceAccount)
      throws Exception {
    this.ingestBucket = ingestBucket;

    gcsUtils.fileExists(wgsVcfPath(ingestBucket));

    DatasetModel dataset = createAndValidateSelfHostedDataset(dedicatedServiceAccount);
    if (dedicatedServiceAccount) {
      ingestServiceAccount = dataset.getIngestServiceAccount();
      authorizeIngestSourceBucket(dataset);
    }

    long expectedTableCount = ingestFilesAndMetadata();

    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
    DatasetIntegrationTest.assertTableCount(bigQuery, dataset, "sample_vcf", expectedTableCount);

    List<Map<String, List<String>>> sampleVcfResults =
        DatasetIntegrationTest.transformStringResults(bigQuery, dataset, "sample_vcf");
    Set<String> ingestedFileIds =
        sampleVcfResults.stream()
            .flatMap(
                result ->
                    Stream.concat(
                        result.get("vcf_file_ref").stream(),
                        result.get("vcf_index_file_ref").stream()))
            .collect(Collectors.toSet());

    assertThat(
        "dataset ingest should result in all fileIds being present in data",
        ingestedFileIds,
        hasSize(directFileIngestTargetPathsToIds.size() + 1));

    assertThat(
        "all the fileIds in direct (non-combined) ingests show up in dataset results",
        ingestedFileIds,
        hasItems(directFileIngestTargetPathsToIds.values().toArray(new String[0])));

    SnapshotSummaryModel snapshot =
        dataRepoFixtures.createSnapshot(
            steward(), dataset.getName(), profileId, "dataset-ingest-combined-array-snapshot.json");
    snapshotId = snapshot.getId();
    snapshotProject = snapshot.getDataProject();

    assertThat(
        "a snapshot created from a self-hosted dataset is also self-hosted",
        snapshot.isSelfHosted(),
        is(true));

    List<DRSObject> collect =
        ingestedFileIds.stream()
            .map(
                fileId -> {
                  try {
                    return dataRepoFixtures.drsGetObject(
                        steward(), String.format("v1_%s_%s", snapshotId, fileId));
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .toList();

    for (DRSObject drsObject : collect) {
      TestUtils.validateDrsAccessMethods(drsObject.getAccessMethods(), stewardToken);
      DRSAccessMethod gsAccessMethod =
          drsObject.getAccessMethods().stream()
              .filter(accessMethod -> accessMethod.getType().equals(DRSAccessMethod.TypeEnum.GS))
              .findFirst()
              .orElseThrow();
      assertThat(
          "the drs object's gs-path is in the original ingest bucket",
          GcsUriUtils.parseBlobUri(gsAccessMethod.getAccessUrl().getUrl()).getBucket(),
          equalTo(ingestBucket));
      DRSAccessURL objectAccessUrl =
          dataRepoFixtures
              .getObjectAccessUrl(steward(), drsObject.getId(), gsAccessMethod.getAccessId())
              .getResponseObject()
              .orElseThrow();

      assertThat(
          "TDR was able to create a signed URL",
          objectAccessUrl.getUrl(),
          is(not(emptyOrNullString())));

      // Ensure that the signed URL is accessible
      TestUtils.verifyHttpAccess(objectAccessUrl.getUrl(), Map.of());
    }

    // validate that snapshot export works correctly
    DataRepoResponse<SnapshotExportResponseModel> exportResponse =
        dataRepoFixtures.exportSnapshotLog(steward(), snapshotId, false, false);
    assertThat(
        "self-hosted snapshots can be exported with DRS URIs",
        exportResponse.getResponseObject().isPresent(),
        is(true));

    DataRepoResponse<JobModel> exportSnapshotExpectFailure =
        dataRepoFixtures.exportSnapshot(steward(), snapshotId, true, false);
    DataRepoResponse<ErrorModel> errorResponse =
        dataRepoClient.waitForResponseLog(
            steward(), exportSnapshotExpectFailure, new TypeReference<>() {});

    assertThat(
        "self-hosted snapshots cannot be exported while resolving DRS URIs to gs-paths",
        errorResponse.getErrorObject().orElseThrow().getMessage(),
        containsString("Cannot export GS Paths for self-hosted snapshots"));

    dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    snapshotId = null;

    Map<String, List<String>> rowToDelete =
        sampleVcfResults.stream()
            .filter(result -> result.get("sample_name").get(0).equals("NA12878_wgs"))
            .findFirst()
            .orElseThrow();
    UUID datarepoRowId = UUID.fromString(rowToDelete.get("datarepo_row_id").get(0));

    dataRepoFixtures.deleteData(
        steward(),
        datasetId,
        new DataDeletionRequest()
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .specType(DataDeletionRequest.SpecTypeEnum.JSONARRAY)
            .tables(
                List.of(
                    new DataDeletionTableModel()
                        .tableName("sample_vcf")
                        .jsonArraySpec(
                            new DataDeletionJsonArrayModel().rowIds(List.of(datarepoRowId))))));

    boolean fileExistsAfterDataDelete = gcsUtils.fileExists(wgsVcfPath(ingestBucket));
    assertThat(
        "files deleted from a self-hosted dataset continue to exist in their source buckets",
        fileExistsAfterDataDelete,
        is(true));

    dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    datasetId = null;

    boolean fileExistsAfterDatasetDelete = gcsUtils.fileExists(exomeVcfPath(ingestBucket));
    assertThat(
        "files still exist even after self-hosted dataset is deleted",
        fileExistsAfterDatasetDelete,
        is(true));
  }

  /**
   * Replace every instance of ${`key`} with `value` in `inputString` for all entries in
   * `replacements` map.
   */
  private String replaceVars(String inputString, Map<String, String> replacements) {
    String outputString = inputString;
    for (var entry : replacements.entrySet()) {
      String placeholder = "\\$\\{%s\\}".formatted(entry.getKey());
      outputString = outputString.replaceAll(placeholder, entry.getValue());
    }
    return outputString;
  }

  private String wgsVcfPath(String ingestBucket) {
    return String.format(
        "gs://%s/selfHostedDatasetTest/vcfs/NA12878_PLUMBING_wgs.g.vcf.gz", ingestBucket);
  }

  private String wgsVcfTargetPath(String suffix) {
    return String.format("/vcfs/downsampled/wgs/NA12878_PLUMBING.g.vcf.gz%s", suffix);
  }

  private String exomeVcfPath(String ingestBucket) {
    return String.format(
        "gs://%s/selfHostedDatasetTest/vcfs/NA12878_PLUMBING_exome.g.vcf.gz", ingestBucket);
  }

  private String exomeVcfTargetPath(String suffix) {
    return String.format("/vcfs/downsampled/exome/NA12878_PLUMBING.g.vcf.gz%s", suffix);
  }

  /**
   * Ingest files and metadata to the self-hosted dataset through all available means:
   *
   * <ul>
   *   <li>Individual file upload
   *   <li>Bulk file upload specified by an array in the request
   *   <li>Bulk file upload specified by a control file
   *   <li>Combined metadata and file ingest
   * </ul>
   *
   * @return expected number of rows in destination table
   */
  private long ingestFilesAndMetadata() throws Exception {
    String singleFileExomeSuffix = "1";
    ingestSingleFile(singleFileExomeSuffix);

    List<String> bulkControlFileExomeSuffixes = List.of("2", "3");
    bulkLoadFilesViaControlFile(bulkControlFileExomeSuffixes);

    bulkLoadFilesViaArray();

    List<String> exomeSuffixes = new ArrayList<>();
    exomeSuffixes.add(singleFileExomeSuffix);
    exomeSuffixes.addAll(bulkControlFileExomeSuffixes);
    return ingestMetadataAndOneCombinedFile(exomeSuffixes);
  }

  private DatasetModel createAndValidateSelfHostedDataset(boolean dedicatedServiceAccount)
      throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createSelfHostedDataset(
            steward(), profileId, "dataset-ingest-combined-array.json", dedicatedServiceAccount);
    datasetId = datasetSummaryModel.getId();

    assertThat(
        "the dataset is marked as self-hosted", datasetSummaryModel.isSelfHosted(), is(true));

    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    assertThat(
        "the dataset returned from the retrieve endpoint is marked as self-hosted",
        dataset.isSelfHosted(),
        is(true));

    return dataset;
  }

  private void authorizeIngestSourceBucket(DatasetModel dataset) {
    ingestServiceAccount = dataset.getIngestServiceAccount();
    // Note: this role gets removed in teardown
    DatasetIntegrationTest.addServiceAccountRoleToBucket(
        ingestBucket, ingestServiceAccount, StorageRoles.objectViewer(), dataset.getDataProject());
    DatasetIntegrationTest.addServiceAccountRoleToBucket(
        ingestBucket,
        ingestServiceAccount,
        StorageRoles.legacyBucketReader(),
        dataset.getDataProject());
  }

  private FileModel ingestSingleFile(String exomeSuffix) throws Exception {
    String targetPath = exomeVcfTargetPath(exomeSuffix);
    FileModel exomeVcfModel =
        dataRepoFixtures.ingestFile(
            steward(), datasetId, profileId, exomeVcfPath(ingestBucket), targetPath);

    directFileIngestTargetPathsToIds.put(targetPath, exomeVcfModel.getFileId());

    return exomeVcfModel;
  }

  private BulkLoadArrayResultModel bulkLoadFilesViaArray() throws Exception {
    List<BulkLoadFileModel> vcfIndexLoadModels =
        List.of(
            new BulkLoadFileModel()
                .mimeType("text/plain")
                .description("A downsampled exome gVCF index")
                .sourcePath(exomeVcfPath(ingestBucket) + ".tbi")
                .targetPath(exomeVcfTargetPath(".tbi")),
            new BulkLoadFileModel()
                .mimeType("text/plain")
                .description("A downsampled wgs gVCF index")
                .sourcePath(wgsVcfPath(ingestBucket) + ".tbi")
                .targetPath(wgsVcfTargetPath(".tbi")));

    BulkLoadArrayRequestModel bulkLoadArrayRequestModel =
        new BulkLoadArrayRequestModel()
            .profileId(profileId)
            .loadArray(vcfIndexLoadModels)
            .maxFailedFileLoads(0)
            .loadTag("selfHostedDataset" + datasetId);

    BulkLoadArrayResultModel result =
        dataRepoFixtures.bulkLoadArray(steward(), datasetId, bulkLoadArrayRequestModel);

    directFileIngestTargetPathsToIds.putAll(
        result.getLoadFileResults().stream()
            .collect(
                Collectors.toMap(
                    BulkLoadFileResultModel::getTargetPath, BulkLoadFileResultModel::getFileId)));

    return result;
  }

  private String exomeBulkLoadControlLine(String suffix) {
    return TestUtils.mapToJson(
        Map.of(
            "description",
            "A downsampled exome gVCF " + suffix,
            "mimeType",
            "text/plain",
            "sourcePath",
            exomeVcfPath(ingestBucket),
            "targetPath",
            exomeVcfTargetPath(suffix)));
  }

  private void bulkLoadFilesViaControlFile(List<String> exomeSuffixes) throws Exception {
    String loadControlFile =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format(
                "selfHostedDatasetTest/%s/self-hosted-bulk-load-file-control.json", datasetId),
            exomeSuffixes.stream().map(this::exomeBulkLoadControlLine));
    uploadedFiles.add(loadControlFile);

    String loadTag = "selfHostedDataset.bulkLoadControl." + datasetId;
    BulkLoadRequestModel bulkLoadControlRequestModel =
        new BulkLoadRequestModel()
            .profileId(profileId)
            .loadControlFile(loadControlFile)
            .maxFailedFileLoads(0)
            .loadTag(loadTag);

    BulkLoadResultModel result =
        dataRepoFixtures.bulkLoad(steward(), datasetId, bulkLoadControlRequestModel);
    assertThat(
        "two files were bulk loaded via control file", result.getSucceededFiles(), equalTo(2));

    BulkLoadHistoryModelList controlFileLoadResults =
        dataRepoFixtures.getLoadHistory(steward(), datasetId, loadTag, 0, 2);
    directFileIngestTargetPathsToIds.putAll(
        controlFileLoadResults.getItems().stream()
            .collect(
                Collectors.toMap(
                    BulkLoadHistoryModel::getTargetPath, BulkLoadHistoryModel::getFileId)));
  }

  private int ingestMetadataAndOneCombinedFile(List<String> exomeSuffixes) throws Exception {
    Map<String, String> exomeFileIdMap =
        exomeSuffixes.stream()
            .collect(
                Collectors.toMap(
                    suffix -> "EXOME%s_VCF_FILE_ID".formatted(suffix),
                    suffix -> directFileIngestTargetPathsToIds.get(exomeVcfTargetPath(suffix))));

    Map<String, String> fileReplaceMap =
        Map.of(
            "INGEST_BUCKET", ingestBucket,
            "EXOME_VCF_INDEX_FILE_ID",
                directFileIngestTargetPathsToIds.get(exomeVcfTargetPath(".tbi")),
            "WGS_VCF_INDEX_FILE_ID",
                directFileIngestTargetPathsToIds.get(wgsVcfTargetPath(".tbi")));

    List<String> lines =
        Files.readAllLines(
            Paths.get(ClassLoader.getSystemResource("self-hosted-dataset-ingest.json").toURI()));
    Stream<String> linesVarsReplaced =
        lines.stream()
            .map(line -> replaceVars(line, exomeFileIdMap))
            .map(line -> replaceVars(line, fileReplaceMap));

    String ingestPath =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("selfHostedDatasetTest/%s/self-hosted-ingest-control.json", datasetId),
            linesVarsReplaced);
    uploadedFiles.add(ingestPath);

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(ingestPath);

    dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    return lines.size();
  }
}
