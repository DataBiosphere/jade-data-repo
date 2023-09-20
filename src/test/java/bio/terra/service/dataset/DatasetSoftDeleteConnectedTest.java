package bio.terra.service.dataset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.resourcemanagement.ResourceService;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class DatasetSoftDeleteConnectedTest {

  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private DatasetDao datasetDao;
  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ResourceService dataLocationService;

  @MockBean private IamProviderInterface samService;

  private DatasetSummaryModel summaryModel;
  private static final Logger logger =
      LoggerFactory.getLogger(DatasetSoftDeleteConnectedTest.class);

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    BillingProfileModel billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    // create a dataset and check that it succeeds
    String resourcePath = "snapshot-test-dataset.json";
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(billingProfile.getId())
        .dedicatedIngestServiceAccount(false);
    summaryModel = connectedOperations.createDataset(datasetRequest);
    logger.info("--------begin test---------");
  }

  @After
  public void tearDown() throws Exception {
    logger.info("--------start of tear down---------");

    configService.reset();
    connectedOperations.teardown();
  }

  // todo known flaky test - documented in DR-1102
  @Test
  public void testRepeatedSoftDelete() throws Exception {
    // load a CSV file that contains the table rows to load into the test bucket
    String resourceFileName = "snapshot-test-dataset-data-row-ids.csv";
    String dirInCloud = "scratch/testRepeatedSoftDelete/" + UUID.randomUUID().toString();
    String tableIngestInputFilePath = uploadIngestInputFile(resourceFileName, dirInCloud);
    // ingest the table
    String tableName = "thetable";
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .csvGenerateRowIds(false)
            .path(tableIngestInputFilePath);
    connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

    // make sure the JSON file gets cleaned up on test teardown
    connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

    // load a CSV file that contains the table rows to soft delete into the test bucket
    String softDeleteRowId = "8c52c63e-8d9f-4cfc-82d0-0f916b2404c1";
    List<String> softDeleteRowIds = new ArrayList<>();
    softDeleteRowIds.add(softDeleteRowId); // add the same rowid twice
    softDeleteRowIds.add(softDeleteRowId);
    DataDeletionRequest softDeleteRequest =
        uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud, "testRepeatedSoftDelete.csv", tableName, softDeleteRowIds);

    // make the soft delete request and wait for it to return
    connectedOperations.softDeleteSuccess(summaryModel.getId(), softDeleteRequest);

    // check that the size of the live table matches what we expect
    List<String> liveTableRowIds1 = getRowIdsFromBQTable(summaryModel.getName(), tableName);
    assertEquals("Size of live table is 3", 3, liveTableRowIds1.size());
    assertFalse(
        "Soft deleted row id is not in live table", liveTableRowIds1.contains(softDeleteRowId));

    // note: the soft delete table name is not exposed to end users, so to check that the state of
    // the
    // soft delete table is correct, I'm reaching into our internals to fetch the table name
    Dataset internalDatasetObj = datasetDao.retrieve(summaryModel.getId());
    DatasetTable internalDatasetTableObj = internalDatasetObj.getTableByName(tableName).get();
    String internalSoftDeleteTableName = internalDatasetTableObj.getSoftDeleteTableName();

    // check that the size of the soft delete table matches what we expect
    List<String> softDeleteRowIds1 =
        getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
    assertEquals("Size of soft delete table is 1", 1, softDeleteRowIds1.size());
    assertTrue(
        "Soft deleted row id is in soft delete table", softDeleteRowIds1.contains(softDeleteRowId));

    // repeat the same soft delete request and wait for it to return
    connectedOperations.softDeleteSuccess(summaryModel.getId(), softDeleteRequest);

    // check that the size of the live table has not changed
    List<String> liveTableRowIds2 = getRowIdsFromBQTable(summaryModel.getName(), tableName);
    assertEquals("Size of live table is still 3", 3, liveTableRowIds2.size());
    assertFalse(
        "Soft deleted row id is still not in live table",
        liveTableRowIds2.contains(softDeleteRowId));

    // check that the size of the soft delete table has not changed
    List<String> softDeleteRowIds2 =
        getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
    assertEquals("Size of soft delete table is still 1", 1, softDeleteRowIds2.size());
    assertTrue(
        "Soft deleted row id is still in soft delete table",
        softDeleteRowIds2.contains(softDeleteRowId));

    // delete the dataset and check that it succeeds
    connectedOperations.deleteTestDatasetAndCleanup(summaryModel.getId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testConcurrentSoftDeletes() throws Exception {
    // load a CSV file that contains the table rows to load into the test bucket
    String resourceFileName = "snapshot-test-dataset-data-row-ids.csv";
    String dirInCloud = "scratch/testConcurrentSoftDeletes/" + UUID.randomUUID().toString();
    String tableIngestInputFilePath = uploadIngestInputFile(resourceFileName, dirInCloud);
    // ingest the table
    String tableName = "thetable";
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .csvGenerateRowIds(false)
            .path(tableIngestInputFilePath);
    connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

    // make sure the JSON file gets cleaned up on test teardown
    connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

    // load CSV file #1 that contains the table rows to soft delete into the test bucket
    String softDeleteRowId1 = "8c52c63e-8d9f-4cfc-82d0-0f916b2404c1";
    DataDeletionRequest softDeleteRequest1 =
        uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud,
            "testConcurrentSoftDeletes1.csv",
            tableName,
            Collections.singletonList(softDeleteRowId1));

    // load CSV file #1 that contains the table rows to soft delete into the test bucket
    String softDeleteRowId2 = "13ae488a-e33f-4ee6-ba30-c1fca4d96b63";
    DataDeletionRequest softDeleteRequest2 =
        uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud,
            "testConcurrentSoftDeletes2.csv",
            tableName,
            Collections.singletonList(softDeleteRowId2));

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DataDeletionStep
    configService.setFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // kick off the first soft delete request, it should hang just before updating the soft delete
    // table
    MvcResult softDeleteResult1 =
        connectedOperations.softDeleteRaw(summaryModel.getId(), softDeleteRequest1);
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // check that the dataset metadata row has a shared lock
    // note: asserts are below outside the hang block
    UUID datasetId = summaryModel.getId();
    String exclusiveLock1 = DatasetDaoUtils.getExclusiveLock(datasetDao, datasetId);
    String[] sharedLocks1 = datasetDao.getSharedLocks(datasetId);

    // kick off the second soft delete request, it should also hang just before updating the soft
    // delete table
    MvcResult softDeleteResult2 =
        connectedOperations.softDeleteRaw(summaryModel.getId(), softDeleteRequest2);
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // check that the dataset metadata row has two shared locks
    // note: asserts are below outside the hang block
    String exclusiveLock2 = DatasetDaoUtils.getExclusiveLock(datasetDao, datasetId);
    String[] sharedLocks2 = datasetDao.getSharedLocks(datasetId);

    // disable hang in DataDeletionStep
    configService.setFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
    // ====================================================

    // check that the dataset metadata row has a shared lock after the first soft delete request was
    // kicked off
    assertNull("dataset row has no exclusive lock", exclusiveLock1);
    assertEquals("dataset row has one shared lock", 1, sharedLocks1.length);

    // check that the dataset metadata row has two shared locks after the second soft delete request
    // was kicked off
    assertNull("dataset row has no exclusive lock", exclusiveLock2);
    assertEquals("dataset row has two shared locks", 2, sharedLocks2.length);

    // wait for the first soft delete to finish and check it succeeded
    MockHttpServletResponse softDeleteResponse1 =
        connectedOperations.validateJobModelAndWait(softDeleteResult1);
    connectedOperations.handleSuccessCase(softDeleteResponse1, DeleteResponseModel.class);

    // wait for the second soft delete to finish and check it succeeded
    MockHttpServletResponse softDeleteResponse2 =
        connectedOperations.validateJobModelAndWait(softDeleteResult2);
    connectedOperations.handleSuccessCase(softDeleteResponse2, DeleteResponseModel.class);

    // check that the size of the live table matches what we expect
    List<String> liveTableRowIds = getRowIdsFromBQTable(summaryModel.getName(), tableName);
    assertEquals("Size of live table is 2", 2, liveTableRowIds.size());
    assertFalse(
        "Soft deleted row id #1 is not in live table", liveTableRowIds.contains(softDeleteRowId1));
    assertFalse(
        "Soft deleted row id #2 is not in live table", liveTableRowIds.contains(softDeleteRowId2));

    // note: the soft delete table name is not exposed to end users, so to check that the state of
    // the
    // soft delete table is correct, I'm reaching into our internals to fetch the table name
    Dataset internalDatasetObj = datasetDao.retrieve(summaryModel.getId());
    DatasetTable internalDatasetTableObj = internalDatasetObj.getTableByName(tableName).get();
    String internalSoftDeleteTableName = internalDatasetTableObj.getSoftDeleteTableName();

    // check that the size of the soft delete table matches what we expect
    List<String> softDeleteRowIds =
        getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
    assertEquals("Size of soft delete table is 2", 2, softDeleteRowIds.size());
    assertTrue(
        "Soft deleted row id #1 is in soft delete table",
        softDeleteRowIds.contains(softDeleteRowId1));
    assertTrue(
        "Soft deleted row id #2 is in soft delete table",
        softDeleteRowIds.contains(softDeleteRowId2));

    // delete the dataset and check that it succeeds
    connectedOperations.deleteTestDatasetAndCleanup(summaryModel.getId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testBadSoftDelete() throws Exception {
    // load a CSV file that contains the table rows to load into the test bucket
    String resourceFileName = "snapshot-test-dataset-data-row-ids.csv";
    String dirInCloud = "scratch/testBadSoftDelete/" + UUID.randomUUID().toString();
    String tableIngestInputFilePath = uploadIngestInputFile(resourceFileName, dirInCloud);

    // ingest the table
    String tableName = "thetable";
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .csvGenerateRowIds(false)
            .path(tableIngestInputFilePath);
    connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

    // make sure the JSON file gets cleaned up on test teardown
    connectedOperations.addScratchFile(dirInCloud + "/" + resourceFileName);

    // load a CSV file that contains the table rows to soft delete into the test bucket
    String softDeleteBadRowId = "badrowid";
    String softDeleteGoodRowId = "8c52c63e-8d9f-4cfc-82d0-0f916b2404c1";
    List<String> softDeleteRowIds = new ArrayList<>();
    softDeleteRowIds.add(softDeleteBadRowId);
    softDeleteRowIds.add(softDeleteGoodRowId);
    DataDeletionRequest softDeleteRequest =
        uploadInputFileAndBuildSoftDeleteRequest(
            dirInCloud, "testBadSoftDelete.csv", tableName, softDeleteRowIds);

    // make the soft delete request and wait for it to return
    MvcResult softDeleteResult =
        connectedOperations.softDeleteRaw(summaryModel.getId(), softDeleteRequest);
    MockHttpServletResponse softDeleteResponse =
        connectedOperations.validateJobModelAndWait(softDeleteResult);
    assertEquals(
        "soft delete of bad row id failed",
        HttpStatus.BAD_REQUEST.value(),
        softDeleteResponse.getStatus());

    // check that the size of the live table matches what we expect
    List<String> liveTableRowIds = getRowIdsFromBQTable(summaryModel.getName(), tableName);
    assertEquals("Size of live table is 4", 4, liveTableRowIds.size());
    assertFalse("Bad row id is not in live table", liveTableRowIds.contains(softDeleteBadRowId));
    assertTrue("Good row id is in live table", liveTableRowIds.contains(softDeleteGoodRowId));

    // note: the soft delete table name is not exposed to end users, so to check that the state of
    // the
    // soft delete table is correct, I'm reaching into our internals to fetch the table name
    Dataset internalDatasetObj = datasetDao.retrieve(summaryModel.getId());
    DatasetTable internalDatasetTableObj = internalDatasetObj.getTableByName(tableName).get();
    String internalSoftDeleteTableName = internalDatasetTableObj.getSoftDeleteTableName();

    // check that the size of the soft delete table matches what we expect
    List<String> softDeleteRowIdsFromBQ =
        getRowIdsFromBQTable(summaryModel.getName(), internalSoftDeleteTableName);
    assertEquals("Size of soft delete table is 0", 0, softDeleteRowIdsFromBQ.size());
    assertFalse(
        "Bad row id is not in soft delete table",
        softDeleteRowIdsFromBQ.contains(softDeleteBadRowId));
    assertFalse(
        "Good row id is not in soft delete table",
        softDeleteRowIdsFromBQ.contains(softDeleteGoodRowId));

    // delete the dataset and check that it succeeds
    connectedOperations.deleteTestDatasetAndCleanup(summaryModel.getId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  private List<String> getRowIdsFromBQTable(String datasetName, String tableName) throws Exception {
    String rowIdColumn = PdaoConstant.PDAO_ROW_ID_COLUMN;
    TableResult bqQueryResult =
        TestUtils.selectFromBigQueryDataset(
            datasetDao, dataLocationService, datasetName, tableName, rowIdColumn);
    List<String> rowIds = new ArrayList<>();
    bqQueryResult.iterateAll().forEach(r -> rowIds.add(r.get(rowIdColumn).getStringValue()));
    return rowIds;
  }

  private DataDeletionRequest uploadInputFileAndBuildSoftDeleteRequest(
      String dirInCloud, String filenameInCloud, String tableName, List<String> softDeleteRowIds)
      throws Exception {
    Storage storage = StorageOptions.getDefaultInstance().getService();

    // load a CSV file that contains the table rows to soft delete into the test bucket
    StringBuilder csvLines = new StringBuilder();
    for (String softDeleteRowId : softDeleteRowIds) {
      csvLines.append(softDeleteRowId + "\n");
    }
    BlobInfo softDeleteBlob =
        BlobInfo.newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + filenameInCloud)
            .build();
    storage.create(softDeleteBlob, csvLines.toString().getBytes(Charset.forName("UTF-8")));
    String softDeleteInputFilePath =
        "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + filenameInCloud;

    // make sure the JSON file gets cleaned up on test teardown
    connectedOperations.addScratchFile(dirInCloud + "/" + filenameInCloud);

    // build the soft delete request with a pointer to a file that contains the row ids to soft
    // delete
    DataDeletionGcsFileModel softDeleteGcsFileModel =
        new DataDeletionGcsFileModel()
            .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
            .path(softDeleteInputFilePath);
    DataDeletionTableModel softDeleteTableModel =
        new DataDeletionTableModel().tableName(tableName).gcsFileSpec(softDeleteGcsFileModel);
    DataDeletionRequest softDeleteRequest =
        new DataDeletionRequest()
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .specType(DataDeletionRequest.SpecTypeEnum.GCSFILE)
            .tables(Arrays.asList(softDeleteTableModel));

    return softDeleteRequest;
  }

  private String uploadIngestInputFile(String resourceFileName, String dirInCloud)
      throws IOException {
    BlobInfo ingestTableBlob =
        BlobInfo.newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + resourceFileName)
            .build();
    Storage storage = StorageOptions.getDefaultInstance().getService();
    storage.create(
        ingestTableBlob,
        IOUtils.toByteArray(getClass().getClassLoader().getResource(resourceFileName)));
    return "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + resourceFileName;
  }
}
