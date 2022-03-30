package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import bio.terra.common.ParquetUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotExportResponseModelFormatParquet;
import bio.terra.model.SnapshotExportResponseModelFormatParquetLocationTables;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class SnapshotExportIntegrationTest extends UsersBase {

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private GcsPdao gcsPdao;
  @Autowired private GoogleBucketService googleBucketService;
  @Autowired private AuthService authService;
  @Rule @Autowired public TestJobWatcher testWatcher;

  @Autowired
  @Qualifier("objectMapper")
  private ObjectMapper objectMapper;

  private static final Logger logger = LoggerFactory.getLogger(SnapshotIntegrationTest.class);
  private String stewardToken;
  private String readerToken;
  private UUID profileId;
  private final List<UUID> createdDatasetsIds = new ArrayList<>();
  private final List<UUID> createdSnapshotIds = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    readerToken = authService.getDirectAccessAuthToken(reader().getEmail());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);
  }

  @After
  public void tearDown() throws Exception {
    for (UUID snapshotId : createdSnapshotIds) {
      try {
        dataRepoFixtures.deleteSnapshot(steward(), snapshotId);
      } catch (Exception ex) {
        logger.warn("cleanup failed when deleting snapshot " + snapshotId);
        ex.printStackTrace();
      }
    }

    for (UUID datasetId : createdDatasetsIds) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void snapshotExportTest() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    UUID datasetId = datasetSummaryModel.getId();
    createdDatasetsIds.add(datasetId);
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            steward(),
            datasetSummaryModel.getName(),
            profileId,
            "ingest-test-snapshot-no-file.json");

    UUID snapshotId = snapshotSummary.getId();
    createdSnapshotIds.add(snapshotId);
    DataRepoResponse<SnapshotExportResponseModel> exportResponse =
        dataRepoFixtures.exportSnapshotLog(steward(), snapshotId, false, true);

    SnapshotExportResponseModel exportModel = exportResponse.getResponseObject().orElseThrow();

    assertThat(
        "The export had it's primary keys validated",
        exportModel.isValidatedPrimaryKeys(),
        is(true));

    SnapshotExportResponseModelFormatParquet parquet = exportModel.getFormat().getParquet();
    SnapshotModel snapshot = exportModel.getSnapshot();

    assertThat("Response contains a parquet format", parquet, notNullValue());
    assertThat(
        "Parquet response has a path to the manifest", parquet.getManifest(), notNullValue());
    assertThat(
        "The response has the correct tables",
        parquet.getLocation().getTables().stream()
            .map(SnapshotExportResponseModelFormatParquetLocationTables::getName)
            .collect(Collectors.toList()),
        containsInAnyOrder("participant", "sample"));

    assertThat("Response contains the snapshot", snapshot.getId(), equalTo(snapshotId));

    BlobId manifestBlob = GcsUriUtils.parseBlobUri(parquet.getManifest());

    String bucketProject = manifestBlob.getBucket().replace("-snapshot-export-bucket", "");
    String manifestContentsRaw =
        gcsPdao
            .getBlobsLinesStream(parquet.getManifest(), bucketProject, null)
            .collect(Collectors.joining("\n"));

    TypeReference<SnapshotExportResponseModel> ref = new TypeReference<>() {};
    SnapshotExportResponseModel manifestContents =
        objectMapper.convertValue(objectMapper.readTree(manifestContentsRaw), ref);

    assertThat(
        "The manifest contains a snapshot model",
        manifestContents.getSnapshot().getId(),
        equalTo(snapshotId));

    assertThat(
        "The manifest and response have the same tables",
        manifestContents.getFormat().getParquet().getLocation().getTables(),
        equalTo(parquet.getLocation().getTables()));

    Integer deleteAge = 1;

    var lifecycleRules =
        googleBucketService.getCloudBucket(manifestBlob.getBucket()).getLifecycleRules();

    var lifecycleRule = lifecycleRules.get(0);
    var lifecycleAction = lifecycleRule.getAction();
    var lifecycleDeleteAge = lifecycleRule.getCondition().getAge();

    assertThat(
        "Delete lifecycle action was set on the bucket",
        lifecycleAction.getActionType(),
        equalTo(BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction().getActionType()));

    assertThat("Delete lifecycle action is set for 1 day", lifecycleDeleteAge, equalTo(deleteAge));

    List<String> paths =
        Stream.concat(
                parquet.getLocation().getTables().stream().flatMap(t -> t.getPaths().stream()),
                Stream.of(parquet.getManifest()))
            .collect(Collectors.toList());

    Storage unauthedStorage =
        StorageOptions.newBuilder()
            .setCredentials(new GoogleCredentials(new AccessToken(readerToken, null)))
            .build()
            .getService();
    Storage authedStorage =
        StorageOptions.newBuilder()
            .setCredentials(new GoogleCredentials(new AccessToken(stewardToken, null)))
            .build()
            .getService();

    for (var path : paths) {
      Blob blob = authedStorage.get(GcsUriUtils.parseBlobUri(parquet.getManifest()));
      assertThat("Authorized user can read " + path, blob, notNullValue());

      StorageException notAuthorizedException = null;
      try {
        unauthedStorage.get(GcsUriUtils.parseBlobUri(parquet.getManifest()));
      } catch (StorageException ex) {
        notAuthorizedException = ex;
      }
      // Appeasing SpotBugs
      assert notAuthorizedException != null;
      assertThat(
          "Unauthorized user cannot read " + path, notAuthorizedException.getCode(), equalTo(403));
    }
  }

  @Test
  public void snapshotGsPathExportTest() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    UUID datasetId = datasetSummaryModel.getId();
    createdDatasetsIds.add(datasetId);

    IngestRequestModel request =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path("gs://jade-testdata-useastregion/snapshot-export-array-fileid.json");

    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            steward(),
            datasetSummaryModel.getName(),
            profileId,
            "dataset-ingest-combined-array-snapshot.json");

    UUID snapshotId = snapshotSummary.getId();
    createdSnapshotIds.add(snapshotId);
    DataRepoResponse<SnapshotExportResponseModel> exportResponse =
        dataRepoFixtures.exportSnapshotLog(steward(), snapshotId, true, false);

    SnapshotExportResponseModel exportModel = exportResponse.getResponseObject().orElseThrow();
    SnapshotExportResponseModelFormatParquet parquet = exportModel.getFormat().getParquet();
    List<String> sampleVcfTablePaths = parquet.getLocation().getTables().get(0).getPaths();
    List<Map<String, Object>> records = new ArrayList<>();
    for (String path : sampleVcfTablePaths) {
      records.addAll(
          ParquetUtils.readGcsParquetRecords(gcsPdao, path, snapshotSummary.getDataProject()));
    }

    for (var record : records) {
      final String vcfFileRef = (String) record.get("vcf_file_ref");
      final List<String> vcfIndexFileRefs = (List<String>) record.get("vcf_index_file_ref");
      if (record.get("sample_name").equals("NA12878_exome")) {
        assertThat(
            "fileref values with multiple elements are mapped as lists of gs-paths",
            vcfIndexFileRefs,
            iterableWithSize(2));
        isGsPath(vcfFileRef);
        vcfIndexFileRefs.forEach(SnapshotExportIntegrationTest::isGsPath);
      } else if (record.get("sample_name").equals("nofile")) {
        assertThat("Null fileref values stay null", vcfFileRef, nullValue());
        assertThat("arrayOf fileref values have no elements", vcfIndexFileRefs, emptyIterable());
      } else {
        isGsPath(vcfFileRef);
        isGsPath(vcfIndexFileRefs.get(0));
      }
    }
  }

  @Test
  public void snapshotExportValidationTest() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    UUID datasetId = datasetSummaryModel.getId();
    createdDatasetsIds.add(datasetId);
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    // Ingest sample table twice to trigger non-unique primary keys
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            steward(), datasetSummaryModel.getName(), profileId, "ingest-test-snapshot.json");

    UUID snapshotId = snapshotSummary.getId();
    createdSnapshotIds.add(snapshotId);
    DataRepoResponse<SnapshotExportResponseModel> exportResponse =
        dataRepoFixtures.exportSnapshotLog(steward(), snapshotId, false, true);

    ErrorModel errorModel = exportResponse.getErrorObject().orElseThrow();

    assertThat(
        "The error says it the export failed validation",
        errorModel.getMessage(),
        not(emptyOrNullString()));

    assertThat(
        "The error detail says there are duplicate primary keys",
        errorModel.getErrorDetail().get(0),
        containsString("1 table(s) had rows with duplicate primary keys"));
  }

  private static void isGsPath(String path) {
    assertThat("Fileref in parquet record is replaced with a gs-path", path, startsWith("gs://"));
  }
}
