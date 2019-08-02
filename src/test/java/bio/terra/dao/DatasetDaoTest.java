package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.ProfileFixtures;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSummary;
import bio.terra.metadata.Table;
import bio.terra.model.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.resourcemanagement.dao.ProfileDao;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

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
    private NamedParameterJdbcTemplate jdbcTemplate;

    private Dataset dataset;
    private UUID datasetId;
    private Dataset fromDB;
    private BillingProfile billingProfile;
    private boolean deleted;

    @Before
    public void setup() throws Exception {
        billingProfile = ProfileFixtures.randomBillingProfile();
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);

        DatasetRequestModel datasetRequest = jsonLoader.loadObject("dataset-create-test.json",
            DatasetRequestModel.class);
        datasetRequest
            .name(datasetRequest.getName() + UUID.randomUUID().toString())
            .defaultProfileId(profileId.toString());
        dataset = DatasetJsonConversion.datasetRequestToDataset(datasetRequest);
        datasetId = datasetDao.create(dataset);
        fromDB = datasetDao.retrieve(datasetId);
    }

    @After
    public void teardown() throws Exception {
        if (!deleted) {
            datasetDao.delete(datasetId);
        }
        datasetId = null;
        fromDB = null;
        profileDao.deleteBillingProfileById(billingProfile.getId());
    }

    private UUID createMinimalDataset() throws IOException {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject("dataset-minimal.json",
            DatasetRequestModel.class);
        datasetRequest
            .name(datasetRequest.getName() + UUID.randomUUID().toString())
            .defaultProfileId(billingProfile.getId().toString());
        return datasetDao.create(DatasetJsonConversion.datasetRequestToDataset(datasetRequest));
    }

    @Test(expected = DatasetNotFoundException.class)
    public void datasetDeleteTest() {
        boolean success = datasetDao.delete(datasetId);
        deleted = success;
        datasetDao.retrieve(datasetId);
    }

    @Test
    public void enumerateTest() throws Exception {
        UUID dataset1 = createMinimalDataset();
        List<UUID> datasetIds = new ArrayList<>();
        datasetIds.add(dataset1);
        datasetIds.add(datasetId);

        MetadataEnumeration<DatasetSummary> summaryEnum = datasetDao.enumerate(0, 2, "created_date",
            "asc", null, datasetIds);
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
            datasetDao.enumerate(1, 1, "created_date", "asc", null, datasetIds)
                .getItems().get(0).getId(),
            equalTo(datasets.get(1).getId()));

        datasetDao.delete(dataset1);
    }

    @Test
    public void datasetTest() throws Exception {
        assertThat("dataset name is set correctly",
                fromDB.getName(),
                equalTo(dataset.getName()));

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
}
