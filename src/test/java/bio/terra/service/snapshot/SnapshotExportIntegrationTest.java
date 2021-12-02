package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.UsersBase;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotExportResponseModelFormatParquet;
import bio.terra.model.SnapshotExportResponseModelFormatParquetLocationTables;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class SnapshotExportIntegrationTest extends UsersBase {
  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Autowired private GcsPdao gcsPdao;

  @Autowired
  @Qualifier("objectMapper")
  private ObjectMapper objectMapper;

  private static final Logger logger = LoggerFactory.getLogger(SnapshotIntegrationTest.class);
  private UUID profileId;
  private UUID datasetId;
  private UUID snapshotId;

  @Before
  public void setup() throws Exception {
    super.setup();
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);

    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot.json", SnapshotRequestModel.class);

    DataRepoResponse<JobModel> createSnapLaunchResp =
        dataRepoFixtures.createSnapshotRaw(
            reader(), datasetSummaryModel.getName(), profileId, requestModel, true, false);
    assertThat(
        "Reader is not authorized on the billing profile to create a dataSnapshot",
        createSnapLaunchResp.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            steward(), datasetSummaryModel.getName(), profileId, "ingest-test-snapshot.json");

    snapshotId = snapshotSummary.getId();
  }

  @After
  public void tearDown() throws Exception {

    if (snapshotId != null) {
      try {
        dataRepoFixtures.deleteSnapshot(steward(), snapshotId);
      } catch (Exception ex) {
        logger.warn("cleanup failed when deleting snapshot " + snapshotId);
        ex.printStackTrace();
      }
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void snapshotExportTest() throws Exception {
    DataRepoResponse<SnapshotExportResponseModel> exportResponse =
        dataRepoFixtures.exportSnapshotLog(steward(), snapshotId);

    SnapshotExportResponseModel exportModel = exportResponse.getResponseObject().get();
    SnapshotExportResponseModelFormatParquet parquet = exportModel.getFormat().getParquet();

    assertThat("Response contains a parquet format", parquet, notNullValue());
    assertThat(
        "Parquet response has a path to the manifest", parquet.getManifest(), notNullValue());
    assertThat(
        "The reponse has the correct number of tables",
        parquet.getLocation().getTables(),
        hasSize(3));

    String bucketProject =
        GcsUriUtils.parseBlobUri(parquet.getManifest())
            .getBucket()
            .replace("-snapshot-export-bucket", "");
    String manifestContents =
        gcsPdao
            .getBlobsLinesStream(parquet.getManifest(), bucketProject, null)
            .collect(Collectors.joining("\n"));

    TypeReference<List<SnapshotExportResponseModelFormatParquetLocationTables>> ref =
        new TypeReference<>() {};
    List<SnapshotExportResponseModelFormatParquetLocationTables> tables =
        objectMapper.convertValue(objectMapper.readTree(manifestContents), ref);

    assertThat(
        "The manifest and response have the same tables",
        tables,
        equalTo(parquet.getLocation().getTables()));
  }
}
