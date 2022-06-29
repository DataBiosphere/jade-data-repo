package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.ras.ECMService;
import bio.terra.service.configuration.ConfigEnum;
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
import java.util.concurrent.TimeUnit;
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
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
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
  @MockBean private ECMService ecmService;

  private BillingProfileModel billingProfile;
  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private DatasetSummaryModel datasetSummary;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    when(ecmService.getRASDbgapPermissions(any())).thenReturn(List.of());
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
  public void testExcludeLockedFromSnapshotLookups() throws Exception {
    // create a snapshot
    SnapshotSummaryModel snapshotSummary =
        connectedOperations.createSnapshot(datasetSummary, "snapshot-test-snapshot.json", "_d2_");

    // check that the snapshot metadata row is unlocked
    String exclusiveLock = snapshotDao.getExclusiveLockState(snapshotSummary.getId());
    assertNull("snapshot row is unlocked", exclusiveLock);

    // retrieve the snapshot and check that it finds it
    SnapshotModel snapshotModel = connectedOperations.getSnapshot(snapshotSummary.getId());
    assertEquals(
        "Lookup unlocked snapshot succeeds", snapshotSummary.getName(), snapshotModel.getName());

    // enumerate snapshots and check that this snapshot is included in the set
    EnumerateSnapshotModel enumerateSnapshotModelModel =
        connectedOperations.enumerateSnapshots(snapshotSummary.getName());
    List<SnapshotSummaryModel> enumeratedSnapshots = enumerateSnapshotModelModel.getItems();
    boolean foundSnapshotWithMatchingId = false;
    for (SnapshotSummaryModel enumeratedSnapshot : enumeratedSnapshots) {
      if (enumeratedSnapshot.getId().equals(snapshotSummary.getId())) {
        foundSnapshotWithMatchingId = true;
        break;
      }
    }
    assertTrue("Unlocked included in enumeration", foundSnapshotWithMatchingId);

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DeleteSnapshotPrimaryDataStep
    configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // kick off a request to delete the snapshot. this should hang before unlocking the snapshot
    // object.
    MvcResult deleteResult =
        mvc.perform(delete("/api/repository/v1/snapshots/" + snapshotSummary.getId())).andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // note: asserts are below outside the hang block
    exclusiveLock = snapshotDao.getExclusiveLockState(snapshotSummary.getId());

    // retrieve the snapshot and check that it returns not found
    // note: asserts are below outside the hang block
    MvcResult retrieveResult =
        mvc.perform(get("/api/repository/v1/snapshots/" + snapshotSummary.getId())).andReturn();

    // enumerate snapshots and check that this snapshot is not included in the set
    // note: asserts are below outside the hang block
    MvcResult enumerateResult =
        connectedOperations.enumerateSnapshotsRaw(snapshotSummary.getName());

    // disable hang in DeleteSnapshotPrimaryDataStep
    configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
    // ====================================================

    // check that the snapshot metadata row has an exclusive lock after kicking off the delete
    assertNotNull("snapshot row is exclusively locked", exclusiveLock);

    // check that the retrieve snapshot returned not found
    connectedOperations.handleFailureCase(retrieveResult.getResponse(), HttpStatus.NOT_FOUND);

    // check that the enumerate snapshots returned successfully and that this snapshot is not
    // included in the set
    enumerateSnapshotModelModel =
        connectedOperations.handleSuccessCase(
            enumerateResult.getResponse(), EnumerateSnapshotModel.class);

    enumeratedSnapshots = enumerateSnapshotModelModel.getItems();
    foundSnapshotWithMatchingId = false;
    for (SnapshotSummaryModel enumeratedSnapshot : enumeratedSnapshots) {
      if (enumeratedSnapshot.getId().equals(snapshotSummary.getId())) {
        foundSnapshotWithMatchingId = true;
        break;
      }
    }
    assertFalse("Exclusively locked not included in enumeration", foundSnapshotWithMatchingId);

    // check the response from the delete request
    MockHttpServletResponse deleteResponse =
        connectedOperations.validateJobModelAndWait(deleteResult);
    DeleteResponseModel deleteResponseModel =
        connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
    assertEquals(
        "Snapshot delete returned successfully",
        DeleteResponseModel.ObjectStateEnum.DELETED,
        deleteResponseModel.getObjectState());

    // try to fetch the snapshot again and confirm nothing is returned
    connectedOperations.getSnapshotExpectError(snapshotSummary.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testExcludeLockedFromSnapshotFileLookups() throws Exception {
    // create a dataset
    DatasetSummaryModel datasetRefSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "simple-with-filerefs-dataset.json");

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

    // create a snapshot
    SnapshotSummaryModel snapshotSummary =
        connectedOperations.createSnapshot(
            datasetRefSummary, "simple-with-filerefs-snapshot.json", "");

    // check that the snapshot metadata row is unlocked
    String exclusiveLock = snapshotDao.getExclusiveLockState(snapshotSummary.getId());
    assertNull("snapshot row is unlocked", exclusiveLock);

    /*
     * WARNING: if making any changes to this test make sure to notify the #dsp-batch channel! Describe the change
     * and any consequences downstream to DRS clients.
     */
    String fileUri = getFileRefIdFromSnapshot(snapshotSummary);
    DrsId drsId = drsIdService.fromUri(fileUri);
    DRSObject drsObject = connectedOperations.drsGetObjectSuccess(drsId.toDrsObjectId(), false);
    String filePath = drsObject.getAliases().get(0);

    // lookup the snapshot file by DRS id, make sure it's returned (lookupSnapshotFileSuccess will
    // already check)
    FileModel fsObjById =
        connectedOperations.lookupSnapshotFileSuccess(
            snapshotSummary.getId(), drsId.getFsObjectId());
    assertEquals(
        "Retrieve snapshot file by DRS id matches desc",
        fsObjById.getDescription(),
        fileLoadModel.getDescription());

    // lookup the snapshot file by DRS path and check that it's found
    FileModel fsObjByPath =
        connectedOperations.lookupSnapshotFileByPathSuccess(snapshotSummary.getId(), filePath, 0);
    assertEquals(
        "Retrieve snapshot file by path matches desc",
        fsObjByPath.getDescription(),
        fileLoadModel.getDescription());
    assertThat(
        "Retrieve snapshot file objects match", fsObjById, CoreMatchers.equalTo(fsObjByPath));

    // now the snapshot exists....let's get it locked!

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DeleteSnapshotPrimaryDataStep
    configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // kick off a request to delete the snapshot. this should hang before unlocking the snapshot
    // object.
    // note: asserts are below outside the hang block
    MvcResult deleteResult =
        mvc.perform(delete("/api/repository/v1/snapshots/" + snapshotSummary.getId())).andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch and get to the hang

    // check that the snapshot metadata row has an exclusive lock
    exclusiveLock = snapshotDao.getExclusiveLockState(snapshotSummary.getId());

    // lookup the snapshot file by id and check that it's NOT found
    MockHttpServletResponse failedGetSnapshotByIdResponse =
        connectedOperations.lookupSnapshotFileRaw(snapshotSummary.getId(), drsId.getFsObjectId());

    // lookup the snapshot file by path and check that it's NOT found
    MockHttpServletResponse failedGetSnapshotByPathResponse =
        connectedOperations.lookupSnapshotFileByPathRaw(snapshotSummary.getId(), filePath, 0);

    // disable hang in DeleteSnapshotPrimaryDataStep
    configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
    // ====================================================

    // check that the snapshot metadata row has an exclusive lock after kicking off the delete
    assertNotNull("snapshot row is exclusively locked", exclusiveLock);

    assertEquals(
        "Snapshot file NOT found by DRS id lookup",
        HttpStatus.NOT_FOUND,
        HttpStatus.valueOf(failedGetSnapshotByIdResponse.getStatus()));

    assertEquals(
        "Snapshot file NOT found by path lookup",
        HttpStatus.NOT_FOUND,
        HttpStatus.valueOf(failedGetSnapshotByPathResponse.getStatus()));

    // check the response from the snapshot delete request
    MockHttpServletResponse deleteResponse =
        connectedOperations.validateJobModelAndWait(deleteResult);
    DeleteResponseModel deleteResponseModel =
        connectedOperations.handleSuccessCase(deleteResponse, DeleteResponseModel.class);
    assertEquals(
        "Snapshot delete returned successfully",
        DeleteResponseModel.ObjectStateEnum.DELETED,
        deleteResponseModel.getObjectState());

    // delete the dataset and check that it succeeds
    connectedOperations.deleteTestDatasetAndCleanup(datasetRefSummary.getId());

    // remove the file from the connectedoperation bookkeeping list
    connectedOperations.removeFile(datasetRefSummary.getId(), fileModel.getFileId());

    // try to fetch the snapshot again and confirm nothing is returned
    connectedOperations.getSnapshotExpectError(snapshotSummary.getId(), HttpStatus.NOT_FOUND);

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(datasetRefSummary.getId(), HttpStatus.NOT_FOUND);
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

    // create a snapshot
    SnapshotSummaryModel snapshotSummary =
        connectedOperations.createSnapshot(
            datasetRefSummary, "simple-with-filerefs-snapshot.json", "");

    // check that the snapshot metadata row is unlocked
    String exclusiveLock = snapshotDao.getExclusiveLockState(snapshotSummary.getId());
    assertNull("snapshot row is unlocked", exclusiveLock);

    /*
     * WARNING: if making any changes to this test make sure to notify the #dsp-batch channel! Describe the change
     * and any consequences downstream to DRS clients.
     */
    String fileUri = getFileRefIdFromSnapshot(snapshotSummary);
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

  private static final String queryForRefIdTemplate =
      "SELECT file_ref FROM `<project>.<snapshot>.<table>` WHERE file_ref IS NOT NULL";

  // Technically a helper method, but so specific to testExcludeLockedFromSnapshotFileLookups,
  // likely not re-useable
  private String getFileRefIdFromSnapshot(SnapshotSummaryModel snapshotSummary)
      throws InterruptedException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotSummary.getName());
    BigQueryProject bigQueryProject =
        BigQueryProject.get(snapshot.getProjectResource().getGoogleProjectId());
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    ST sqlTemplate = new ST(queryForRefIdTemplate);
    sqlTemplate.add("project", snapshot.getProjectResource().getGoogleProjectId());
    sqlTemplate.add("snapshot", snapshot.getName());
    sqlTemplate.add("table", "tableA");

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
