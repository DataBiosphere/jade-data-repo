package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.ras.EcmService;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.stringtemplate.v4.ST;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class SnapshotFileLookupConnectedTest {

  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private ConfigurationService configService;
  @Autowired private MockMvc mvc;
  @Autowired private DrsIdService drsIdService;
  @Autowired private JsonLoader jsonLoader;

  @MockBean private IamProviderInterface samService;
  @MockBean private EcmService ecmService;

  private BillingProfileModel billingProfile;
  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private DatasetSummaryModel datasetSummary;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    when(ecmService.getRasDbgapPermissions(any())).thenReturn(List.of());
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    datasetSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "snapshot-test-dataset.json");
    SnapshotConnectedTestUtils.loadCsvData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetSummary.getId(),
        "thetable",
        "snapshot-test-dataset-data-row-ids.csv");
  }

  @After
  public void tearDown() throws Exception {
    connectedOperations.teardown();
    configService.reset();
  }

  @Test
  public void testSnapshotArrayFileRefLookups() throws Exception {
    // create a dataset
    DatasetSummaryModel datasetRefSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "simple-with-array-filerefs-dataset.json");

    // ingest a file
    URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
    String targetFilePath =
        "/mm/" + Names.randomizeName("testdir") + "/testExcludeLockedFromSnapshotFileLookups.txt";
    UUID descriptionRandomUUID = UUID.randomUUID();
    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description(descriptionRandomUUID.toString())
            .mimeType("text/plain")
            .targetPath(targetFilePath)
            .profileId(billingProfile.getId());
    FileModel fileModel =
        connectedOperations.ingestFileSuccess(datasetRefSummary.getId(), fileLoadModel);

    // generate a JSON file with the fileref
    String jsonLine = "{\"name\":\"name1\", \"file_ref\":[\"" + fileModel.getFileId() + "\"]}\n";

    // load a JSON file that contains the table rows to load into the test bucket
    String jsonFileName = "this-array-better-pass.json";
    String dirInCloud = "scratch/testExcludeLockedFromSnapshotFileLookups/" + UUID.randomUUID();
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

    // create a snapshot
    SnapshotSummaryModel snapshotSummary =
        connectedOperations.createSnapshot(
            datasetRefSummary, "simple-with-filerefs-snapshot.json", "");

    // check that the snapshot metadata row is unlocked
    assertNull("snapshot row is unlocked", snapshotSummary.getLockingJobId());

    /*
     * WARNING: if making any changes to this test make sure to notify the #dsp-batch channel! Describe the change
     * and any consequences downstream to DRS clients.
     */
    String fileUri = getFileRefIdFromSnapshot(snapshotSummary, "file_ref");
    DrsId drsId = drsIdService.fromUri(fileUri);
    DRSObject drsObject = connectedOperations.drsGetObjectSuccess(drsId.toDrsObjectId(), false);
    String filePath = drsObject.getAliases().get(0);

    // lookup the snapshot file by DRS id, make sure it's returned (lookupSnapshotFileSuccess will
    // already check)
    FileModel fsObjById =
        connectedOperations.lookupSnapshotFileSuccess(
            snapshotSummary.getId(), drsId.getFsObjectId());
    assertEquals(
        "Retrieve snapshot file by DRS id matches uuid in desc",
        fsObjById.getDescription(),
        fileLoadModel.getDescription());

    // lookup the snapshot file by DRS path and check that it's found
    FileModel fsObjByPath =
        connectedOperations.lookupSnapshotFileByPathSuccess(snapshotSummary.getId(), filePath, 0);
    assertEquals(
        "Retrieve snapshot file by path matches uuid in desc",
        fsObjByPath.getDescription(),
        fileLoadModel.getDescription());
    assertThat(
        "Retrieve snapshot file objects match", fsObjById, CoreMatchers.equalTo(fsObjByPath));
  }

  // Note: the standard DRS URI mode gets tested with most DRS access methods
  @Test
  public void testDrsWithCompactId() throws Exception {
    // create a dataset
    DatasetSummaryModel datasetRefSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations,
            billingProfile,
            "simple-with-array-and-single-filerefs-dataset.json");

    // ingest a file
    URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
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

    // generate a JSON file with the filerefs
    String jsonLine =
        "{\"name\":\"name1\", \"file_ref\":\"%s\", \"file_ref_a\": [\"%s\"]}\n"
            .formatted(fileModel.getFileId(), fileModel.getFileId());

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

    SnapshotRequestModel snapshotRequest =
        jsonLoader
            .loadObject("simple-with-filerefs-snapshot.json", SnapshotRequestModel.class)
            .compactIdPrefix("foo.0");
    // create a snapshot
    SnapshotSummaryModel snapshotSummary =
        connectedOperations.createSnapshot(datasetRefSummary, snapshotRequest, "");

    // Iterate over file fields to verify DRS ids are valid
    for (String fieldName : List.of("file_ref", "file_ref_a")) {
      String fileUri = getFileRefIdFromSnapshot(snapshotSummary, fieldName);
      DrsId drsId = DrsIdService.fromUri(fileUri);
      DRSObject drsObject = connectedOperations.drsGetObjectSuccess(drsId.toDrsObjectId(), false);

      assertEquals(
          "Retrieve snapshot file by DRS id matches desc",
          drsObject.getDescription(),
          fileLoadModel.getDescription());
    }
  }

  private static final String queryForRefIdTemplate =
      "SELECT <fieldName> FROM `<project>.<snapshot>.<table>` WHERE file_ref IS NOT NULL";

  // Technically a helper method, but so specific to testExcludeLockedFromSnapshotFileLookups,
  // likely not re-useable
  private String getFileRefIdFromSnapshot(SnapshotSummaryModel snapshotSummary, String fieldName)
      throws InterruptedException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotSummary.getName());
    BigQueryProject bigQueryProject =
        BigQueryProject.get(snapshot.getProjectResource().getGoogleProjectId());
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    ST sqlTemplate = new ST(queryForRefIdTemplate);
    sqlTemplate.add("project", snapshot.getProjectResource().getGoogleProjectId());
    sqlTemplate.add("snapshot", snapshot.getName());
    sqlTemplate.add("table", "tableA");
    sqlTemplate.add("fieldName", fieldName);

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlTemplate.render()).build();
    TableResult result = bigQuery.query(queryConfig);
    FieldValueList row = result.iterateAll().iterator().next();
    FieldValue idValue = row.get(0);
    if (idValue.getAttribute().name().equals("REPEATED")) {
      return idValue.getRepeatedValue().stream()
          .map(FieldValue::getStringValue)
          .collect(Collectors.toList())
          .get(0);
    } else {
      return idValue.getStringValue();
    }
  }
}
