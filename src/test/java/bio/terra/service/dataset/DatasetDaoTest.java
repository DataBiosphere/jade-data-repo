package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Table;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
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
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetDaoTest {

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private GoogleResourceDao resourceDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private BillingProfileModel billingProfile;
    private UUID projectId;

    private UUID createDataset(DatasetRequestModel datasetRequest, String newName) throws Exception {
        datasetRequest.name(newName).defaultProfileId(billingProfile.getId());
        Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
        dataset.projectResourceId(projectId);
        String createFlightId = UUID.randomUUID().toString();
        UUID datasetId = UUID.randomUUID();
        dataset.id(datasetId);
        datasetDao.createAndLock(dataset, createFlightId);
        datasetDao.unlockExclusive(dataset.getId(), createFlightId);
        return datasetId;
    }

    private UUID createDataset(String datasetFile) throws Exception  {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
        return createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID().toString());
    }

    private boolean checkDatasetStorageContainsRegion(DatasetSummary datasetSummary, String region) {
        Dataset dataset = datasetDao.retrieve(datasetSummary.getId());
        return dataset.getDatasetSummary()
            .getStorage()
            .stream()
            .anyMatch(sr -> sr.getRegion().toString().equals(region));
    }

    @Before
    public void setup() {
        BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
        billingProfile = profileDao.createBillingProfile(profileRequest, "hi@hi.hi");

        GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
        projectId = resourceDao.createProject(projectResource);
    }

    @After
    public void teardown() {
        resourceDao.deleteProject(projectId);
        profileDao.deleteBillingProfileById(UUID.fromString(billingProfile.getId()));
    }

    @Test(expected = DatasetNotFoundException.class)
    public void datasetDeleteTest() throws Exception {
        UUID datasetId = createDataset("dataset-minimal.json");
        assertThat("dataset delete signals success", datasetDao.delete(datasetId), equalTo(true));
        datasetDao.retrieve(datasetId);
    }

    @Test
    public void enumerateTest() throws Exception {
        UUID dataset1 = createDataset("dataset-minimal.json");
        UUID dataset2 = createDataset("ingest-test-dataset-east.json");
        List<UUID> datasetIds = new ArrayList<>();
        datasetIds.add(dataset1);
        datasetIds.add(dataset2);
        Dataset dataset1FromDB = datasetDao.retrieve(dataset1);

        MetadataEnumeration<DatasetSummary> summaryEnum = datasetDao.enumerate(0, 2,
            EnumerateSortByParam.CREATED_DATE, SqlSortDirection.ASC, null, datasetIds);
        List<DatasetSummary> datasets = summaryEnum.getItems();
        assertThat("dataset enumerate limit param works",
            datasets.size(),
            equalTo(2));

        assertThat("dataset enumerate returns datasets in the order created",
            datasets.get(0).getCreatedDate().toEpochMilli(),
                Matchers.lessThan(datasets.get(1).getCreatedDate().toEpochMilli()));

        // this is skipping the first item returned above
        // so compare the id from the previous retrieve
        assertThat("dataset enumerate offset param works",
            datasetDao.enumerate(1, 1, EnumerateSortByParam.CREATED_DATE, SqlSortDirection.ASC,
                null, datasetIds)
                .getItems().get(0).getId(),
            equalTo(datasets.get(1).getId()));

        MetadataEnumeration<DatasetSummary> filterNameEnum = datasetDao.enumerate(0, 2,
            EnumerateSortByParam.CREATED_DATE, SqlSortDirection.ASC, dataset1FromDB.getName(), datasetIds);
        List<DatasetSummary> filteredDatasets = filterNameEnum.getItems();
        assertThat("dataset filter by name returns correct total",
            filteredDatasets.size(),
            equalTo(1));
        assertThat("dataset filter by name returns correct dataset",
            filteredDatasets.get(0).getName(),
            equalTo(dataset1FromDB.getName()));

        MetadataEnumeration<DatasetSummary> filterDefaultRegionEnum = datasetDao.enumerate(0, 2,
            EnumerateSortByParam.CREATED_DATE, SqlSortDirection.ASC, GoogleRegion.US_CENTRAL1.toString(), datasetIds);
        List<DatasetSummary> filteredDefaultRegionDatasets = filterDefaultRegionEnum.getItems();
        assertThat("dataset filter by default GCS region returns correct total",
            filteredDefaultRegionDatasets.size(),
            equalTo(2));
        assertThat("dataset filter by default GCS region returns correct datasets",
            filteredDefaultRegionDatasets.stream()
                .allMatch(datasetSummary ->
                    checkDatasetStorageContainsRegion(datasetSummary, GoogleRegion.US_CENTRAL1.toString())),
            equalTo(true));

        MetadataEnumeration<DatasetSummary> filterRegionEnum = datasetDao.enumerate(0, 2,
            EnumerateSortByParam.CREATED_DATE, SqlSortDirection.ASC, GoogleRegion.US_EAST1.toString(), datasetIds);
        List<DatasetSummary> filteredRegionDatasets = filterRegionEnum.getItems();
        assertThat("dataset filter by non-default GCS region returns correct total",
            filteredRegionDatasets.size(),
            equalTo(1));
        assertThat("dataset filter by non-default region returns correct dataset",
            filteredRegionDatasets.stream()
                .allMatch(datasetSummary ->
                    checkDatasetStorageContainsRegion(datasetSummary, GoogleRegion.US_EAST1.toString())),
            equalTo(true));

        datasetDao.delete(dataset1);
        datasetDao.delete(dataset2);
    }

    @Test
    public void datasetTest() throws Exception {
        DatasetRequestModel request = jsonLoader.loadObject("dataset-create-test.json", DatasetRequestModel.class);
        String expectedName = request.getName() + UUID.randomUUID().toString();

        UUID datasetId = createDataset(request, expectedName);
        try {
            Dataset fromDB = datasetDao.retrieve(datasetId);

            assertThat("dataset name is set correctly",
                fromDB.getName(),
                equalTo(expectedName));

            // verify tables
            assertThat("correct number of tables created for dataset",
                fromDB.getTables().size(),
                equalTo(2));
            fromDB.getTables().forEach(this::assertDatasetTable);

            assertThat("correct number of relationships are created for dataset",
                fromDB.getRelationships().size(),
                equalTo(2));

            assertTablesInRelationship(fromDB);

            // verify assets
            assertThat("correct number of assets created for dataset",
                fromDB.getAssetSpecifications().size(),
                equalTo(2));
            fromDB.getAssetSpecifications().forEach(this::assertAssetSpecs);

            Map<GoogleCloudResource, StorageResource> storageMap = fromDB.getDatasetSummary().getStorage().stream()
                .collect(Collectors.toMap(StorageResource::getCloudResource, Function.identity()));

            for (GoogleCloudResource cloudResource : GoogleCloudResource.values()) {
                StorageResource storage = storageMap.get(cloudResource);
                assertThat(String.format("dataset %s region is set", storage.getCloudResource()),
                    storage.getRegion().toString(),
                    equalTo("us-central1"));
            }
        } finally {
            datasetDao.delete(datasetId);
        }
    }

    @Test
    public void partitionTest() throws Exception {
        UUID datasetId = createDataset("ingest-test-partitioned-dataset.json");
        try {
            Dataset fromDB = datasetDao.retrieve(datasetId);
            DatasetTable participants = fromDB.getTableByName("participant")
                .orElseThrow(IllegalStateException::new);
            DatasetTable samples = fromDB.getTableByName("sample")
                .orElseThrow(IllegalStateException::new);
            DatasetTable files = fromDB.getTableByName("file")
                .orElseThrow(IllegalStateException::new);

            assertThat("int-range partition settings are persisted",
                participants.getBigQueryPartitionConfig(),
                equalTo(BigQueryPartitionConfigV1.intRange("age", 0, 120, 1)));
            assertThat("date partition settings are persisted",
                samples.getBigQueryPartitionConfig(),
                equalTo(BigQueryPartitionConfigV1.date("date_collected")));
            assertThat("ingest-time partition settings are persisted",
                files.getBigQueryPartitionConfig(),
                equalTo(BigQueryPartitionConfigV1.ingestDate()));
        } finally {
            datasetDao.delete(datasetId);
        }
    }

    @Test
    public void primaryKeyTest() throws Exception {
        UUID datasetId = createDataset("dataset-primary-key.json");
        try {
            Dataset fromDB = datasetDao.retrieve(datasetId);
            DatasetTable variants = fromDB.getTableByName("variant").orElseThrow(IllegalStateException::new);
            DatasetTable freqAnalysis =
                fromDB.getTableByName("frequency_analysis").orElseThrow(IllegalStateException::new);
            DatasetTable metaAnalysis = fromDB.getTableByName("meta_analysis").orElseThrow(IllegalStateException::new);

            assertThat("single-column primary keys are set correctly",
                variants.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()),
                equalTo(Collections.singletonList("id")));

            assertThat("dual-column primary keys are set correctly",
                metaAnalysis.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()),
                equalTo(Arrays.asList("variant_id", "phenotype")));

            assertThat("many-column primary keys are set correctly",
                freqAnalysis.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList()),
                equalTo(Arrays.asList("variant_id", "ancestry", "phenotype")));
        } finally {
            datasetDao.delete(datasetId);
        }
    }

    protected void assertTablesInRelationship(Dataset dataset) {
        String sqlFrom = "SELECT from_table "
                + "FROM dataset_relationship WHERE id = :id";
        String sqlTo = "SELECT to_table "
                + "FROM dataset_relationship WHERE id = :id";
        dataset.getRelationships().stream().forEach(rel -> {
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", rel.getId());
            UUID fromUUID = jdbcTemplate.queryForObject(sqlFrom, params, UUID.class);
            assertThat("from table id in DB matches that in retrieved object",
                    fromUUID,
                    equalTo(rel.getFromColumn().getTable().getId()));
            UUID toUUID = jdbcTemplate.queryForObject(sqlTo, params, UUID.class);
            assertThat("to table id in DB matches that in retrieved object",
                    toUUID,
                    equalTo(rel.getToColumn().getTable().getId()));
        });
    }

    protected void assertDatasetTable(Table table) {
        if (table.getName().equals("participant")) {
            assertThat("participant table has 4 columns",
                    table.getColumns().size(),
                    equalTo(4));
        } else {
            assertThat("other table created is sample",
                    table.getName(),
                    equalTo("sample"));
            assertThat("sample table has 3 columns",
                    table.getColumns().size(),
                    equalTo(3));
        }

    }

    protected void assertAssetSpecs(AssetSpecification spec) {
        if (spec.getName().equals("Trio")) {
            assertThat("Trio asset has 2 tables",
                    spec.getAssetTables().size(),
                    equalTo(2));
            assertThat("participant is the root table for Trio",
                    spec.getRootTable().getTable().getName(),
                    equalTo("participant"));
            assertThat("participant asset table has only 3 columns",
                    spec.getRootTable().getColumns().size(),
                    equalTo(3));
            assertThat("Trio asset follows 2 relationships",
                    spec.getAssetRelationships().size(),
                    equalTo(2));
        } else {
            assertThat("other asset created is Sample",
                    spec.getName(),
                    equalTo("sample"));
            assertThat("Sample asset has 2 tables",
                    spec.getAssetTables().size(),
                    equalTo(2));
            assertThat("sample is the root table",
                    spec.getRootTable().getTable().getName(),
                    equalTo("sample"));
            assertThat("and 3 columns",
                    spec.getRootTable().getColumns().size(),
                    equalTo(3));
            assertThat("Sample asset follows 1 relationship",
                    spec.getAssetRelationships().size(),
                    equalTo(1));
        }
    }

    @Test
    public void mixingSharedAndExclusiveLocksTest() throws Exception {
        UUID datasetId = createDataset("dataset-primary-key.json");
        try {
            // check that there are no outstanding locks
            String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after creation", exclusiveLock);
            String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after creation", 0, sharedLocks.length);

            // 1. take out a shared lock
            // confirm that there are no exclusive locks and one shared lock
            datasetDao.lockShared(datasetId, "flightid1");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after step 1", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("one shared lock after step 1", 1, sharedLocks.length);
            assertEquals("flightid1 has shared lock after step 1", "flightid1", sharedLocks[0]);

            // 2. take out another shared lock
            // confirm that there are no exclusive locks and two shared locks
            datasetDao.lockShared(datasetId, "flightid2");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after step 2", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("two shared locks after step 2", 2, sharedLocks.length);
            assertTrue("flightid2 has shared lock after step 2",
                Arrays.asList(sharedLocks).contains("flightid2"));

            // 3. try to take out an exclusive lock
            // confirm that it fails with a DatasetLockException
            boolean threwLockException = false;
            try {
                datasetDao.lockExclusive(datasetId, "flightid3");
            } catch (DatasetLockException dlEx) {
                threwLockException = true;
            }
            assertTrue("exclusive lock threw exception in step 3", threwLockException);

            // 4. release the first shared lock
            // confirm that there are no exclusive locks and one shared lock
            datasetDao.unlockShared(datasetId, "flightid1");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after step 4", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("one shared lock after step 4", 1, sharedLocks.length);
            assertFalse("flightid1 no longer has shared lock after step 4",
                Arrays.asList(sharedLocks).contains("flightid1"));

            // 5. try to take out an exclusive lock
            // confirm that it fails with a DatasetLockException
            threwLockException = false;
            try {
                datasetDao.lockExclusive(datasetId, "flightid4");
            } catch (DatasetLockException dlEx) {
                threwLockException = true;
            }
            assertTrue("exclusive lock threw exception in step 5", threwLockException);

            // 6. take out five shared locks
            // confirm that there are no exclusive locks and six shared locks
            datasetDao.lockShared(datasetId, "flightid5");
            datasetDao.lockShared(datasetId, "flightid6");
            datasetDao.lockShared(datasetId, "flightid7");
            datasetDao.lockShared(datasetId, "flightid8");
            datasetDao.lockShared(datasetId, "flightid9");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after step 6", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("six shared locks after step 6", 6, sharedLocks.length);
            assertTrue("flightid2 has shared lock after step 6",
                Arrays.asList(sharedLocks).contains("flightid2"));
            assertTrue("flightid5 has shared lock after step 6",
                Arrays.asList(sharedLocks).contains("flightid5"));
            assertTrue("flightid6 has shared lock after step 6",
                Arrays.asList(sharedLocks).contains("flightid6"));
            assertTrue("flightid7 has shared lock after step 6",
                Arrays.asList(sharedLocks).contains("flightid7"));
            assertTrue("flightid8 has shared lock after step 6",
                Arrays.asList(sharedLocks).contains("flightid8"));
            assertTrue("flightid9 has shared lock after step 6",
                Arrays.asList(sharedLocks).contains("flightid9"));

            // 7. release all the shared locks
            // confirm that there are no outstanding locks
            datasetDao.unlockShared(datasetId, "flightid2");
            datasetDao.unlockShared(datasetId, "flightid5");
            datasetDao.unlockShared(datasetId, "flightid6");
            datasetDao.unlockShared(datasetId, "flightid7");
            datasetDao.unlockShared(datasetId, "flightid8");
            datasetDao.unlockShared(datasetId, "flightid9");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after step 7", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 7", 0, sharedLocks.length);

            // 8. take out an exclusive lock
            // confirm that there is an exclusive lock and no shared locks
            datasetDao.lockExclusive(datasetId, "flightid10");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertEquals("exclusive lock taken out after step 8", "flightid10", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 8", 0, sharedLocks.length);

            // 9. try to take out a shared lock
            // confirm that it fails with a DatasetLockException
            threwLockException = false;
            try {
                datasetDao.lockShared(datasetId, "flightid11");
            } catch (DatasetLockException dlEx) {
                threwLockException = true;
            }
            assertTrue("shared lock threw exception in step 9", threwLockException);

            // 10. release the exclusive lock
            // confirm that there are no outstanding locks
            datasetDao.unlockExclusive(datasetId, "flightid10");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("exclusive lock taken out after step 10", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 10", 0, sharedLocks.length);

        } finally {
            datasetDao.delete(datasetId);
        }
    }

    @Test
    public void duplicateCallsForExclusiveLockTest() throws Exception {
        UUID datasetId = createDataset("dataset-primary-key.json");
        try {
            // check that there are no outstanding locks
            String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after creation", exclusiveLock);
            String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after creation", 0, sharedLocks.length);

            // 1. take out an exclusive lock
            // confirm that there is an exclusive lock and no shared locks
            datasetDao.lockExclusive(datasetId, "flightid20");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertEquals("exclusive lock taken out after step 1", "flightid20", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 1", 0, sharedLocks.length);

            // 2. try to take out an exclusive lock again with the same flightid
            // confirm that the exclusive lock is still there and there are no shared locks
            datasetDao.lockExclusive(datasetId, "flightid20");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertEquals("exclusive lock taken out after step 2", "flightid20", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 2", 0, sharedLocks.length);

            // 3. try to unlock the exclusive lock with a different flightid
            // confirm that the exclusive lock is still there and there are no shared locks
            boolean rowUnlocked = datasetDao.unlockExclusive(datasetId, "flightid21");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertFalse("no rows updated on call to unlock with different flightid after step 3", rowUnlocked);
            assertEquals("exclusive lock still taken out after step 3", "flightid20", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 3", 0, sharedLocks.length);

            // 4. unlock the exclusive lock
            // confirm that there are no outstanding exclusive or shared locks
            rowUnlocked = datasetDao.unlockExclusive(datasetId, "flightid20");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertTrue("row was updated on first call to unlock after step 4", rowUnlocked);
            assertNull("no exclusive lock after step 4", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 4", 0, sharedLocks.length);

            // 5. unlock the exclusive lock again with the same flightid
            // confirm that there are still no oustanding exclusive or shared locks
            rowUnlocked = datasetDao.unlockExclusive(datasetId, "flightid20");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertFalse("no rows updated on second call to unlock after step 5", rowUnlocked);
            assertNull("no exclusive lock after step 5", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 5", 0, sharedLocks.length);
        } finally {
            datasetDao.delete(datasetId);
        }
    }

    @Test
    public void duplicateCallsForSharedLockTest() throws Exception {
        UUID datasetId = createDataset("dataset-primary-key.json");
        try {
            // check that there are no outstanding locks
            String exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after creation", exclusiveLock);
            String[] sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after creation", 0, sharedLocks.length);

            // 1. take out a shared lock
            // confirm that there is no exclusive lock and one shared lock
            datasetDao.lockShared(datasetId, "flightid30");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after step 1", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("one shared lock after step 1", 1, sharedLocks.length);
            assertEquals("flightid30 has shared lock after step 1", "flightid30", sharedLocks[0]);

            // 2. try to take out a shared lock again with the same flightid
            // confirm that the shared lock is still there and there is no exclusive lock
            datasetDao.lockShared(datasetId, "flightid30");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertNull("no exclusive lock after step 2", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("one shared lock after step 2", 1, sharedLocks.length);
            assertEquals("flightid30 has shared lock after step 2", "flightid30", sharedLocks[0]);

            // 3. try to unlock the shared lock with a different flightid
            // confirm that the shared lock is still there and there is no exclusive lock
            boolean rowUnlocked = datasetDao.unlockShared(datasetId, "flightid31");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertFalse("no rows updated on call to unlock with different flightid after step 3", rowUnlocked);
            assertNull("no exclusive lock after step 3", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("one shared lock still taken out after step 3", 1, sharedLocks.length);
            assertEquals("flightid30 still has shared lock after step 3", "flightid30", sharedLocks[0]);

            // 4. unlock the shared lock
            // confirm that there are no outstanding exclusive or shared locks
            rowUnlocked = datasetDao.unlockShared(datasetId, "flightid30");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertTrue("row was updated on first call to unlock after step 4", rowUnlocked);
            assertNull("no exclusive lock after step 4", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 4", 0, sharedLocks.length);

            // 5. unlock the exclusive lock again with the same flightid
            // confirm that there are still no oustanding exclusive or shared locks
            rowUnlocked = datasetDao.unlockShared(datasetId, "flightid30");
            exclusiveLock = datasetDao.getExclusiveLock(datasetId);
            assertFalse("no rows updated on second call to unlock after step 5", rowUnlocked);
            assertNull("no exclusive lock after step 5", exclusiveLock);
            sharedLocks = datasetDao.getSharedLocks(datasetId);
            assertEquals("no shared locks after step 5", 0, sharedLocks.length);
        } finally {
            datasetDao.delete(datasetId);
        }
    }

    @Test
    public void lockNonExistentDatasetTest() throws Exception {
        UUID nonExistentDatasetId = UUID.randomUUID();

        // try to take out an exclusive lock
        // confirm that it fails with a DatasetNotFoundkException
        boolean threwNotFoundException = false;
        try {
            datasetDao.lockExclusive(nonExistentDatasetId, "flightid40");
        } catch (DatasetNotFoundException dnfEx) {
            threwNotFoundException = true;
        }
        assertTrue("exclusive lock threw not found exception", threwNotFoundException);

        // try to release an exclusive lock
        // confirm that it succeeds with no rows updated
        boolean rowUpdated = datasetDao.unlockExclusive(nonExistentDatasetId, "flightid40");
        assertFalse("exclusive unlock did not update any rows", rowUpdated);

        // try to take out a shared lock
        // confirm that it fails with a DatasetNotFoundkException
        threwNotFoundException = false;
        try {
            datasetDao.lockShared(nonExistentDatasetId, "flightid41");
        } catch (DatasetNotFoundException dnfEx) {
            threwNotFoundException = true;
        }
        assertTrue("exclusive lock threw not found exception", threwNotFoundException);

        // try to release a shared lock
        // confirm that it succeeds with no rows updated
        rowUpdated = datasetDao.unlockExclusive(nonExistentDatasetId, "flightid40");
        assertFalse("shared unlock did not update any rows", rowUpdated);
    }
}
