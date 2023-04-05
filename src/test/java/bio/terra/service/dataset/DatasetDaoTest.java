package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Table;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class DatasetDaoTest {

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DatasetDao datasetDao;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private NamedParameterJdbcTemplate jdbcTemplate;

  private BillingProfileModel billingProfile;
  private UUID projectId;
  private final List<UUID> datasetIdsToDelete = new ArrayList<>();

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private UUID createDataset(
      DatasetRequestModel datasetRequest, String newName, GoogleRegion region) throws Exception {
    datasetRequest
        .name(newName)
        .cloudPlatform(CloudPlatform.GCP)
        .defaultProfileId(billingProfile.getId());
    if (region != null) {
      datasetRequest.region(region.toString()).cloudPlatform(CloudPlatform.GCP);
    }
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectId);
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId, TEST_USER);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);
    datasetIdsToDelete.add(datasetId);
    return datasetId;
  }

  private UUID createDataset(String datasetFile) throws Exception {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
    return createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID(), null);
  }

  @Before
  public void setup() {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "hi@hi.hi");

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    projectId = resourceDao.createProject(projectResource);

    datasetIdsToDelete.clear();
  }

  @After
  public void teardown() {
    for (UUID datasetId : datasetIdsToDelete) {
      assertTrue(
          "Dataset %s was deleted".formatted(datasetId), datasetDao.delete(datasetId, TEST_USER));
      assertThrows(
          "Dataset %s cannot be retrieved after deletion".formatted(datasetId),
          DatasetNotFoundException.class,
          () -> datasetDao.retrieve(datasetId));
    }
    resourceDao.deleteProject(projectId);
    profileDao.deleteBillingProfileById(billingProfile.getId());
  }

  @Test
  public void enumerateTest() throws Exception {
    UUID dataset1 = createDataset("dataset-minimal.json");
    createDataset("ingest-test-dataset-east.json");
    Dataset dataset1FromDB = datasetDao.retrieve(dataset1);

    MetadataEnumeration<DatasetSummary> summaryEnum =
        datasetDao.enumerate(
            0, 2, EnumerateSortByParam.CREATED_DATE, SqlSortDirection.ASC, null, null, datasetIds);
    List<DatasetSummary> datasets = summaryEnum.getItems();
    assertThat("dataset enumerate limit param works", datasets, hasSize(2));

    assertThat(
        "dataset enumerate returns datasets in the order created",
        datasets.get(0).getCreatedDate().toEpochMilli(),
        Matchers.lessThan(datasets.get(1).getCreatedDate().toEpochMilli()));

    for (var datasetSummary : datasets) {
      assertThat(
          "dataset summary has the GCP cloud platform",
          datasetSummary.getCloudPlatform(),
          equalTo(CloudPlatform.GCP));
      assertThat(
          "dataset summary has a data project", datasetSummary.getDataProject(), notNullValue());
    }

    // this is skipping the first item returned above
    // so compare the id from the previous retrieve
    assertThat(
        "dataset enumerate offset param works",
        datasetDao
            .enumerate(
                1,
                1,
                EnumerateSortByParam.CREATED_DATE,
                SqlSortDirection.ASC,
                null,
                null,
                datasetIds)
            .getItems()
            .get(0)
            .getId(),
        equalTo(datasets.get(1).getId()));

    MetadataEnumeration<DatasetSummary> filterNameEnum =
        datasetDao.enumerate(
            0,
            2,
            EnumerateSortByParam.CREATED_DATE,
            SqlSortDirection.ASC,
            dataset1FromDB.getName(),
            null,
            datasetIds);
    List<DatasetSummary> filteredDatasets = filterNameEnum.getItems();
    assertThat("dataset filter by name returns correct total", filteredDatasets.size(), equalTo(1));
    assertThat(
        "dataset filter by name returns correct dataset",
        filteredDatasets.get(0).getName(),
        equalTo(dataset1FromDB.getName()));

    MetadataEnumeration<DatasetSummary> filterDefaultRegionEnum =
        datasetDao.enumerate(
            0,
            2,
            EnumerateSortByParam.CREATED_DATE,
            SqlSortDirection.ASC,
            null,
            GoogleRegion.DEFAULT_GOOGLE_REGION.toString(),
            datasetIds);
    List<DatasetSummary> filteredDefaultRegionDatasets = filterDefaultRegionEnum.getItems();
    assertThat(
        "dataset filter by default GCS region returns correct total",
        filteredDefaultRegionDatasets,
        hasSize(1));
    assertTrue(
        "dataset filter by default GCS region returns correct datasets",
        filteredDefaultRegionDatasets.stream()
            .allMatch(
                datasetSummary ->
                    datasetSummary.datasetStorageContainsRegion(
                        GoogleRegion.DEFAULT_GOOGLE_REGION)));

    MetadataEnumeration<DatasetSummary> filterNameAndRegionEnum =
        datasetDao.enumerate(
            0,
            2,
            EnumerateSortByParam.CREATED_DATE,
            SqlSortDirection.ASC,
            dataset1FromDB.getName(),
            GoogleRegion.DEFAULT_GOOGLE_REGION.toString(),
            datasetIds);
    List<DatasetSummary> filteredNameAndRegionDatasets = filterNameAndRegionEnum.getItems();
    assertThat(
        "dataset filter by name and region returns correct size results",
        filteredNameAndRegionDatasets,
        hasSize(1));
    assertThat(
        "dataset filter by name and region returns dataset with correct name",
        filteredNameAndRegionDatasets.get(0).getName(),
        equalTo(dataset1FromDB.getName()));
    assertTrue(
        "dataset filter by name and region returns dataset with correct region",
        filteredNameAndRegionDatasets.stream()
            .allMatch(
                datasetSummary ->
                    datasetSummary.datasetStorageContainsRegion(
                        GoogleRegion.DEFAULT_GOOGLE_REGION)));

    assertThat(
        "dataset filter by name and region returns correct total",
        filterNameAndRegionEnum.getTotal(),
        equalTo(2));
    assertThat(
        "dataset filter by name and region returns correct filtered total",
        filterNameAndRegionEnum.getFilteredTotal(),
        equalTo(1));

    MetadataEnumeration<DatasetSummary> filterRegionEnum =
        datasetDao.enumerate(
            0,
            2,
            EnumerateSortByParam.CREATED_DATE,
            SqlSortDirection.ASC,
            null,
            GoogleRegion.US_EAST1.toString(),
            datasetIds);
    List<DatasetSummary> filteredRegionDatasets = filterRegionEnum.getItems();
    assertThat(
        "dataset filter by non-default GCS region returns correct total",
        filteredRegionDatasets,
        hasSize(1));
    assertTrue(
        "dataset filter by non-default region returns correct dataset",
        filteredRegionDatasets.stream()
            .allMatch(
                datasetSummary ->
                    datasetSummary.datasetStorageContainsRegion(GoogleRegion.US_EAST1)));
  }

  @Test
  public void datasetTest() throws Exception {
    DatasetRequestModel request =
        jsonLoader.loadObject("dataset-create-test.json", DatasetRequestModel.class);
    String expectedName = request.getName() + UUID.randomUUID();

    GoogleRegion testSettingRegion = GoogleRegion.ASIA_NORTHEAST1;
    UUID datasetId = createDataset(request, expectedName, testSettingRegion);
    Dataset fromDB = datasetDao.retrieve(datasetId);

    assertThat("dataset name is set correctly", fromDB.getName(), equalTo(expectedName));

    // verify tables
    assertThat("correct number of tables created for dataset", fromDB.getTables(), hasSize(2));
    fromDB.getTables().forEach(this::assertDatasetTable);

    assertThat(
        "correct number of relationships are created for dataset",
        fromDB.getRelationships(),
        hasSize(2));

    assertTablesInRelationship(fromDB);

    // verify assets
    assertThat(
        "correct number of assets created for dataset",
        fromDB.getAssetSpecifications(),
        hasSize(2));

    fromDB.getAssetSpecifications().forEach(this::assertAssetSpecs);

    for (GoogleCloudResource resource : GoogleCloudResource.values()) {
      CloudRegion region = fromDB.getDatasetSummary().getStorageResourceRegion(resource);
      assertThat(
          String.format("dataset %s region is set", resource), region, equalTo(testSettingRegion));
    }

    assertThat(
        "dataset has billing profiles returned from the database",
        fromDB.getDatasetSummary().getBillingProfiles(),
        is(not(empty())));

    assertThat(
        "dataset default Billing Profile matches default profile id",
        fromDB.getDatasetSummary().getDefaultBillingProfile().getId(),
        equalTo(fromDB.getDatasetSummary().getDefaultProfileId()));

    assertThat("Cloud platform is returned", fromDB.getCloudPlatform(), equalTo(CloudPlatform.GCP));
  }

  @Test
  public void datasetRegionFirestoreFallbackTest() throws Exception {
    DatasetRequestModel request =
        jsonLoader.loadObject("dataset-create-test.json", DatasetRequestModel.class).region("US");
    String expectedName = request.getName() + UUID.randomUUID();

    UUID datasetId = createDataset(request, expectedName, GoogleRegion.US);
    Dataset fromDB = datasetDao.retrieve(datasetId);

    for (GoogleCloudResource resource : GoogleCloudResource.values()) {
      CloudRegion region =
          (GoogleRegion) fromDB.getDatasetSummary().getStorageResourceRegion(resource);
      GoogleRegion expectedRegion =
          (resource == GoogleCloudResource.BIGQUERY) ? GoogleRegion.US : GoogleRegion.US_EAST4;
      assertThat(
          String.format("dataset %s region is set to %s", resource, region.name()),
          region,
          equalTo(expectedRegion));
    }
  }

  @Test
  public void partitionTest() throws Exception {
    UUID datasetId = createDataset("ingest-test-partitioned-dataset.json");
    Dataset fromDB = datasetDao.retrieve(datasetId);
    DatasetTable participants =
        fromDB.getTableByName("participant").orElseThrow(IllegalStateException::new);
    DatasetTable samples = fromDB.getTableByName("sample").orElseThrow(IllegalStateException::new);
    DatasetTable files = fromDB.getTableByName("file").orElseThrow(IllegalStateException::new);

    assertThat(
        "int-range partition settings are persisted",
        participants.getBigQueryPartitionConfig(),
        equalTo(BigQueryPartitionConfigV1.intRange("age", 0, 120, 1)));
    assertThat(
        "date partition settings are persisted",
        samples.getBigQueryPartitionConfig(),
        equalTo(BigQueryPartitionConfigV1.date("date_collected")));
    assertThat(
        "ingest-time partition settings are persisted",
        files.getBigQueryPartitionConfig(),
        equalTo(BigQueryPartitionConfigV1.ingestDate()));
  }

  @Test
  public void primaryKeyTest() throws Exception {
    UUID datasetId = createDataset("dataset-primary-key.json");
    Dataset fromDB = datasetDao.retrieve(datasetId);
    DatasetTable variants =
        fromDB.getTableByName("variant").orElseThrow(IllegalStateException::new);
    DatasetTable freqAnalysis =
        fromDB.getTableByName("frequency_analysis").orElseThrow(IllegalStateException::new);
    DatasetTable metaAnalysis =
        fromDB.getTableByName("meta_analysis").orElseThrow(IllegalStateException::new);

    assertThat(
        "single-column primary keys are set correctly",
        variants.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()),
        equalTo(Collections.singletonList("id")));

    assertThat(
        "dual-column primary keys are set correctly",
        metaAnalysis.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()),
        equalTo(Arrays.asList("variant_id", "phenotype")));

    assertThat(
        "many-column primary keys are set correctly",
        freqAnalysis.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()),
        equalTo(Arrays.asList("variant_id", "ancestry", "phenotype")));
  }

  protected void assertTablesInRelationship(Dataset dataset) {
    String sqlFrom = "SELECT from_table " + "FROM dataset_relationship WHERE id = :id";
    String sqlTo = "SELECT to_table " + "FROM dataset_relationship WHERE id = :id";
    dataset
        .getRelationships()
        .forEach(
            rel -> {
              MapSqlParameterSource params =
                  new MapSqlParameterSource().addValue("id", rel.getId());
              UUID fromUUID = jdbcTemplate.queryForObject(sqlFrom, params, UUID.class);
              assertThat(
                  "from table id in DB matches that in retrieved object",
                  fromUUID,
                  equalTo(rel.getFromColumn().getTable().getId()));
              UUID toUUID = jdbcTemplate.queryForObject(sqlTo, params, UUID.class);
              assertThat(
                  "to table id in DB matches that in retrieved object",
                  toUUID,
                  equalTo(rel.getToColumn().getTable().getId()));
            });
  }

  protected void assertDatasetTable(Table table) {
    if (table.getName().equals("participant")) {
      assertThat("participant table has 4 columns", table.getColumns(), hasSize(4));
    } else {
      assertThat("other table created is sample", table.getName(), equalTo("sample"));
      assertThat("sample table has 3 columns", table.getColumns(), hasSize(3));
    }
  }

  protected void assertAssetSpecs(AssetSpecification spec) {
    if (spec.getName().equals("Trio")) {
      assertThat("Trio asset has 2 tables", spec.getAssetTables(), hasSize(2));
      assertThat(
          "participant is the root table for Trio",
          spec.getRootTable().getTable().getName(),
          equalTo("participant"));
      assertThat(
          "participant asset table has only 3 columns",
          spec.getRootTable().getColumns(),
          hasSize(3));
      assertThat("Trio asset follows 2 relationships", spec.getAssetRelationships(), hasSize(2));
    } else {
      assertThat("other asset created is Sample", spec.getName(), equalTo("sample"));
      assertThat("Sample asset has 2 tables", spec.getAssetTables(), hasSize(2));
      assertThat(
          "sample is the root table", spec.getRootTable().getTable().getName(), equalTo("sample"));
      assertThat("and 3 columns", spec.getRootTable().getColumns(), hasSize(3));
      assertThat("Sample asset follows 1 relationship", spec.getAssetRelationships(), hasSize(1));
    }
  }

  @Test
  public void mixingSharedAndExclusiveLocksTest() throws Exception {
    UUID datasetId = createDataset("dataset-primary-key.json");

    // check that there are no outstanding locks
    String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after creation", exclusiveLock);
    List<String> sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after creation", sharedLocks, empty());

    // 1. take out a shared lock
    // confirm that there are no exclusive locks and one shared lock
    String sharedLock1 = "flightid1";
    datasetDao.lockShared(datasetId, sharedLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after step 1", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("one shared lock after step 1", sharedLocks, hasSize(1));
    assertEquals("flightid1 has shared lock after step 1", sharedLocks.get(0), sharedLock1);

    // 2. take out another shared lock
    // confirm that there are no exclusive locks and two shared locks
    String sharedLock2 = "flightid2";
    datasetDao.lockShared(datasetId, sharedLock2);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after step 2", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("two shared locks after step 2", sharedLocks, hasSize(2));
    assertTrue("flightid2 has shared lock after step 2", sharedLocks.contains(sharedLock2));

    // 3. try to take out an exclusive lock
    // confirm that it fails with a DatasetLockException
    assertThrows(
        "exclusive lock threw exception in step 3",
        DatasetLockException.class,
        () -> datasetDao.lockExclusive(datasetId, "flightid3"));

    // 4. release the first shared lock
    // confirm that there are no exclusive locks and one shared lock
    datasetDao.unlockShared(datasetId, sharedLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after step 4", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("one shared lock after step 4", sharedLocks, hasSize(1));
    assertFalse(
        "flightid1 no longer has shared lock after step 4", sharedLocks.contains(sharedLock1));

    // 5. try to take out an exclusive lock
    // confirm that it fails with a DatasetLockException
    assertThrows(
        "exclusive lock threw exception in step 5",
        DatasetLockException.class,
        () -> datasetDao.lockExclusive(datasetId, "flightid4"));

    // 6. take out five shared locks
    // confirm that there are no exclusive locks and six shared locks
    List<String> bulkSharedLocks =
        IntStream.range(5, 10).mapToObj("flightid%d"::formatted).toList();

    bulkSharedLocks.forEach(sharedLock -> datasetDao.lockShared(datasetId, sharedLock));

    List<String> expectedSharedLocks = new ArrayList<>();
    expectedSharedLocks.add(sharedLock2);
    expectedSharedLocks.addAll(bulkSharedLocks);

    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after step 6", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat(
        "all expected shared locks after step 6",
        sharedLocks,
        containsInAnyOrder(expectedSharedLocks.toArray()));

    // 7. release all the shared locks
    // confirm that there are no outstanding locks
    expectedSharedLocks.forEach(sharedLock -> datasetDao.unlockShared(datasetId, sharedLock));
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after step 7", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 7", sharedLocks, empty());

    // 8. take out an exclusive lock
    // confirm that there is an exclusive lock and no shared locks
    String expectedExclusiveLock = "flightid10";
    datasetDao.lockExclusive(datasetId, expectedExclusiveLock);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertEquals("exclusive lock taken out after step 8", expectedExclusiveLock, exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 8", sharedLocks, empty());

    // 9. try to take out a shared lock
    // confirm that it fails with a DatasetLockException
    assertThrows(
        "shared lock threw exception in step 9",
        DatasetLockException.class,
        () -> datasetDao.lockShared(datasetId, "flightid11"));

    // 10. release the exclusive lock
    // confirm that there are no outstanding locks
    datasetDao.unlockExclusive(datasetId, exclusiveLock);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("exclusive lock taken out after step 10", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 10", sharedLocks, empty());
  }

  @Test
  public void duplicateCallsForExclusiveLockTest() throws Exception {
    UUID datasetId = createDataset("dataset-primary-key.json");

    // check that there are no outstanding locks
    String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after creation", exclusiveLock);
    List<String> sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after creation", sharedLocks, empty());

    // 1. take out an exclusive lock
    // confirm that there is an exclusive lock and no shared locks
    String exclusiveLock1 = "flightId20";
    datasetDao.lockExclusive(datasetId, exclusiveLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertEquals("exclusive lock taken out after step 1", exclusiveLock, exclusiveLock1);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 1", sharedLocks, empty());

    // 2. try to take out an exclusive lock again with the same flightid
    // confirm that the exclusive lock is still there and there are no shared locks
    datasetDao.lockExclusive(datasetId, exclusiveLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertEquals("exclusive lock taken out after step 2", exclusiveLock, exclusiveLock1);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 2", sharedLocks, empty());

    // 3. try to unlock the exclusive lock with a different flightid
    // confirm that the exclusive lock is still there and there are no shared locks
    String exclusiveLock2 = "flightId21";
    boolean rowUnlocked = datasetDao.unlockExclusive(datasetId, exclusiveLock2);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertFalse(
        "no rows updated on call to unlock with different flightid after step 3", rowUnlocked);
    assertEquals("exclusive lock still taken out after step 3", exclusiveLock, exclusiveLock1);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 3", sharedLocks, empty());

    // 4. unlock the exclusive lock
    // confirm that there are no outstanding exclusive or shared locks
    rowUnlocked = datasetDao.unlockExclusive(datasetId, exclusiveLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertTrue("row was updated on first call to unlock after step 4", rowUnlocked);
    assertNull("no exclusive lock after step 4", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 4", sharedLocks, empty());

    // 5. unlock the exclusive lock again with the same flightid
    // confirm that there are still no outstanding exclusive or shared locks
    rowUnlocked = datasetDao.unlockExclusive(datasetId, exclusiveLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertFalse("no rows updated on second call to unlock after step 5", rowUnlocked);
    assertNull("no exclusive lock after step 5", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 5", sharedLocks, empty());
  }

  @Test
  public void duplicateCallsForSharedLockTest() throws Exception {
    UUID datasetId = createDataset("dataset-primary-key.json");

    // check that there are no outstanding locks
    String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after creation", exclusiveLock);
    List<String> sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after creation", sharedLocks, empty());

    // 1. take out a shared lock
    // confirm that there is no exclusive lock and one shared lock
    String sharedLock1 = "flightid30";
    datasetDao.lockShared(datasetId, sharedLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after step 1", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("one shared lock after step 1", sharedLocks, hasSize(1));
    assertEquals("flightid30 has shared lock after step 1", sharedLocks.get(0), sharedLock1);

    // 2. try to take out a shared lock again with the same flightid
    // confirm that the shared lock is still there and there is no exclusive lock
    datasetDao.lockShared(datasetId, sharedLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertNull("no exclusive lock after step 2", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("one shared lock after step 2", sharedLocks, hasSize(1));
    assertEquals("flightid30 has shared lock after step 2", sharedLocks.get(0), sharedLock1);

    // 3. try to unlock the shared lock with a different flightid
    // confirm that the shared lock is still there and there is no exclusive lock
    String sharedLock2 = "flightid31";
    boolean rowUnlocked = datasetDao.unlockShared(datasetId, sharedLock2);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertFalse(
        "no rows updated on call to unlock with different flightid after step 3", rowUnlocked);
    assertNull("no exclusive lock after step 3", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("one shared lock after step 3", sharedLocks, hasSize(1));
    assertEquals("flightid30 still has shared lock after step 3", sharedLocks.get(0), sharedLock1);

    // 4. unlock the shared lock
    // confirm that there are no outstanding exclusive or shared locks
    rowUnlocked = datasetDao.unlockShared(datasetId, sharedLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertTrue("row was updated on first call to unlock after step 4", rowUnlocked);
    assertNull("no exclusive lock after step 4", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 4", sharedLocks, empty());

    // 5. unlock the exclusive lock again with the same flightid
    // confirm that there are still no outstanding exclusive or shared locks
    rowUnlocked = datasetDao.unlockShared(datasetId, sharedLock1);
    exclusiveLock = datasetDao.getExclusiveLock(datasetId);
    assertFalse("no rows updated on second call to unlock after step 5", rowUnlocked);
    assertNull("no exclusive lock after step 5", exclusiveLock);
    sharedLocks = Arrays.asList(datasetDao.getSharedLocks(datasetId));
    assertThat("no shared locks after step 5", sharedLocks, empty());
  }

  @Test
  public void lockNonExistentDatasetTest() {
    UUID nonExistentDatasetId = UUID.randomUUID();

    // try to take out an exclusive lock
    // confirm that it fails with a DatasetNotFoundException
    String exclusiveLock = "flightid40";
    assertThrows(
        "exclusive lock threw not found exception",
        DatasetNotFoundException.class,
        () -> datasetDao.lockExclusive(nonExistentDatasetId, exclusiveLock));

    // try to release an exclusive lock
    // confirm that it succeeds with no rows updated
    boolean rowUpdated = datasetDao.unlockExclusive(nonExistentDatasetId, exclusiveLock);
    assertFalse("exclusive unlock did not update any rows", rowUpdated);

    // try to take out a shared lock
    // confirm that it fails with a DatasetNotFoundException
    String sharedLock = "flightid41";
    assertThrows(
        "shared lock threw not found exception",
        DatasetNotFoundException.class,
        () -> datasetDao.lockShared(nonExistentDatasetId, sharedLock));

    // try to release a shared lock
    // confirm that it succeeds with no rows updated
    rowUpdated = datasetDao.unlockExclusive(nonExistentDatasetId, sharedLock);
    assertFalse("shared unlock did not update any rows", rowUpdated);
  }

  @Test
  public void rowMetadataTable() throws Exception {
    UUID datasetId = createDataset("dataset-minimal.json");
    Dataset dataset = datasetDao.retrieve(datasetId);
    dataset
        .getTables()
        .forEach(
            t ->
                assertThat(
                    "can retrieve row metadata table name",
                    t.getRowMetadataTableName(),
                    containsString("row_metadata")));
  }

  @Test
  public void columnOrderCreationTest() throws Exception {
    String dsName1 = "ds1";
    String dsName2 = "ds2";
    String tableName = "myTable";
    DatasetRequestModel requestModel1 =
        new DatasetRequestModel()
            .name(dsName1)
            .schema(
                new DatasetSpecificationModel()
                    .addTablesItem(
                        new TableModel()
                            .name(tableName)
                            .addColumnsItem(
                                new ColumnModel().name("c1").datatype(TableDataType.STRING))
                            .addColumnsItem(
                                new ColumnModel().name("c2").datatype(TableDataType.STRING))
                            .addColumnsItem(
                                new ColumnModel().name("c3").datatype(TableDataType.STRING))));
    UUID dataset1Id = createDataset(requestModel1, dsName1, null);

    DatasetRequestModel requestModel2 =
        new DatasetRequestModel()
            .name(dsName1)
            .schema(
                new DatasetSpecificationModel()
                    .addTablesItem(
                        new TableModel()
                            .name(tableName)
                            // Note the reversed order
                            .addColumnsItem(
                                new ColumnModel().name("c3").datatype(TableDataType.STRING))
                            .addColumnsItem(
                                new ColumnModel().name("c2").datatype(TableDataType.STRING))
                            .addColumnsItem(
                                new ColumnModel().name("c1").datatype(TableDataType.STRING))));

    UUID dataset2Id = createDataset(requestModel2, dsName2, null);

    assertThat(
        "First dataset columns are in ascending order of name",
        datasetDao
            .retrieve(dataset1Id)
            .getTableByName(tableName)
            .orElseThrow(AssertionError::new)
            .getColumns()
            .stream()
            .map(Column::getName)
            .toList(),
        contains("c1", "c2", "c3"));

    assertThat(
        "Second dataset columns are in descending order of name",
        datasetDao
            .retrieve(dataset2Id)
            .getTableByName(tableName)
            .orElseThrow(AssertionError::new)
            .getColumns()
            .stream()
            .map(Column::getName)
            .toList(),
        contains("c3", "c2", "c1"));
  }

  @Test
  public void patchDatasetPhsId() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");

    assertNull("dataset's PHS ID is null before patch", datasetDao.retrieve(datasetId).getPhsId());

    String phsIdSet = "phs000000";
    DatasetPatchRequestModel patchRequestSet = new DatasetPatchRequestModel().phsId(phsIdSet);
    datasetDao.patch(datasetId, patchRequestSet, TEST_USER);
    assertThat(
        "dataset's PHS ID is set from patch",
        datasetDao.retrieve(datasetId).getPhsId(),
        equalTo(phsIdSet));

    String phsIdOverride = "phs111111";
    DatasetPatchRequestModel patchRequestOverride =
        new DatasetPatchRequestModel().phsId(phsIdOverride);
    datasetDao.patch(datasetId, patchRequestOverride, TEST_USER);
    assertThat(
        "dataset's PHS ID is overridden from patch",
        datasetDao.retrieve(datasetId).getPhsId(),
        equalTo(phsIdOverride));

    datasetDao.patch(datasetId, new DatasetPatchRequestModel(), TEST_USER);
    assertThat(
        "dataset's PHS ID is unchanged when unspecified in patch request",
        datasetDao.retrieve(datasetId).getPhsId(),
        equalTo(phsIdOverride));

    DatasetPatchRequestModel patchRequestBlank = new DatasetPatchRequestModel().phsId("");
    datasetDao.patch(datasetId, patchRequestBlank, TEST_USER);
    assertThat(
        "dataset's PHS ID is set to empty string from patch",
        datasetDao.retrieve(datasetId).getPhsId(),
        equalTo(""));
  }

  @Test
  public void createDatasetWithProperties() throws Exception {
    DatasetRequestModel request =
        jsonLoader.loadObject("dataset-create-with-properties.json", DatasetRequestModel.class);
    String expectedName = request.getName() + UUID.randomUUID();
    UUID datasetId = createDataset(request, expectedName, null);

    assertThat(
        "dataset properties are set",
        datasetDao.retrieve(datasetId).getProperties(),
        equalTo(request.getProperties()));

  }

  @Test
  public void updatePredictableFileIdsFlag() throws Exception {
    UUID datasetId = createDataset("dataset-minimal.json");
    assertFalse(
        "predictable file ids flag is false",
        datasetDao.retrieve(datasetId).hasPredictableFileIds());
    datasetDao.setPredictableFileId(datasetId, true);
    assertTrue(
        "predictable file ids flag is true",
        datasetDao.retrieve(datasetId).hasPredictableFileIds());
  }

  @Test
  public void patchDatasetProperties() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");

    String defaultDesc = datasetDao.retrieve(datasetId).getDescription();
    assertNull(
        "dataset properties is null before patch", datasetDao.retrieve(datasetId).getProperties());

    String updatedProperties = "{\"projectName\":\"updatedProject\"}";
    Object updatedDatasetProperties =
        jsonLoader.loadJson(updatedProperties, new TypeReference<>() {});
    DatasetPatchRequestModel patchRequestSet =
        new DatasetPatchRequestModel().properties(updatedDatasetProperties);
    datasetDao.patch(datasetId, patchRequestSet, TEST_USER);
    assertThat(
        "dataset properties is set from patch",
        datasetDao.retrieve(datasetId).getProperties(),
        equalTo(updatedDatasetProperties));
    assertThat(
        "description remains unchanged after the patch",
        datasetDao.retrieve(datasetId).getDescription(),
        equalTo(defaultDesc));

    DatasetPatchRequestModel patchRequestNull = new DatasetPatchRequestModel().phsId("phs123");
    datasetDao.patch(datasetId, patchRequestNull, TEST_USER);
    assertThat(
        "dataset properties is unchanged when not in request",
        datasetDao.retrieve(datasetId).getProperties(),
        equalTo(updatedDatasetProperties));
    assertThat(
        "dataset description is unchanged when not in request",
        datasetDao.retrieve(datasetId).getDescription(),
        equalTo(defaultDesc));

    DatasetPatchRequestModel patchRequestExplicitNull =
        new DatasetPatchRequestModel().properties(null);
    datasetDao.patch(datasetId, patchRequestExplicitNull, TEST_USER);
    assertThat(
        "dataset properties is unchanged if set to null",
        datasetDao.retrieve(datasetId).getProperties(),
        equalTo(updatedDatasetProperties));

    DatasetPatchRequestModel patchRequestDescNull =
        new DatasetPatchRequestModel().description(null);
    datasetDao.patch(datasetId, patchRequestDescNull, TEST_USER);
    assertThat(
        "dataset description is unchanged if set to null",
        datasetDao.retrieve(datasetId).getDescription(),
        equalTo(defaultDesc));

    String originalPhsID = datasetDao.retrieve(datasetId).getPhsId();
    Object originalProperties = datasetDao.retrieve(datasetId).getProperties();
    DatasetPatchRequestModel patchRequestDesc =
        new DatasetPatchRequestModel().description("A new description");
    datasetDao.patch(datasetId, patchRequestDesc, TEST_USER);
    assertThat(
        "dataset description is updated",
        datasetDao.retrieve(datasetId).getDescription(),
        equalTo("A new description"));
    assertThat(
        "dataset phs_id is not modified when changing description",
        datasetDao.retrieve(datasetId).getPhsId(),
        equalTo(originalPhsID));
    assertThat(
        "dataset properties are not modified when changing description",
        datasetDao.retrieve(datasetId).getProperties(),
        equalTo(originalProperties));

    Object unsetDatasetProperties = jsonLoader.loadJson("{}", new TypeReference<>() {});
    DatasetPatchRequestModel patchRequestUnset =
        new DatasetPatchRequestModel().properties(unsetDatasetProperties);
    datasetDao.patch(datasetId, patchRequestUnset, TEST_USER);
    assertThat(
        "dataset properties is set to empty",
        datasetDao.retrieve(datasetId).getProperties(),
        equalTo(unsetDatasetProperties));
  }

  @Test
  public void createDatasetWithTags() throws Exception {
    DatasetRequestModel requestNullTags =
        jsonLoader.loadObject("dataset-create-test.json", DatasetRequestModel.class).tags(null);
    String nameNullTags = requestNullTags.getName() + UUID.randomUUID();
    UUID datasetIdNullTags = createDataset(requestNullTags, nameNullTags, null);

    verifyTags("null dataset tags are returned as empty list", datasetIdNullTags, List.of());

    DatasetRequestModel requestEmptyTags =
        jsonLoader
            .loadObject("dataset-create-test.json", DatasetRequestModel.class)
            .tags(List.of());
    String nameEmptyTags = requestEmptyTags.getName() + UUID.randomUUID();
    UUID datasetIdEmptyTags = createDataset(requestEmptyTags, nameEmptyTags, null);

    verifyTags("empty dataset tags are returned as empty list", datasetIdEmptyTags, List.of());

    List<String> tags = new ArrayList<>(List.of("a tag", "A TAG", "duplicate", "duplicate"));
    tags.add(null);
    List<String> expectedTags = List.of("a tag", "A TAG", "duplicate");
    DatasetRequestModel requestWithTags =
        jsonLoader.loadObject("dataset-create-test.json", DatasetRequestModel.class).tags(tags);
    String nameWithTags = requestWithTags.getName() + UUID.randomUUID();
    UUID datasetIdWithTags = createDataset(requestWithTags, nameWithTags, null);

    verifyTags("distinct non-null tags are returned", datasetIdWithTags, expectedTags);
  }

  private void verifyTags(String reason, UUID datasetId, List<String> expectedTags) {
    assertThat(
        reason + " when retrieving dataset",
        datasetDao.retrieve(datasetId).getTags(),
        containsInAnyOrder(expectedTags.toArray()));

    assertThat(
        reason + " when retrieving dataset summary",
        datasetDao.retrieveSummaryById(datasetId).getTags(),
        containsInAnyOrder(expectedTags.toArray()));
  }
}
