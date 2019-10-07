package bio.terra.service.dataset;

import bio.terra.category.Unit;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Table;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.resourcemanagement.ProfileDao;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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

    private BillingProfile billingProfile;

    private UUID createDataset(DatasetRequestModel datasetRequest, String newName) {
        datasetRequest.name(newName).defaultProfileId(billingProfile.getId().toString());
        return datasetDao.create(DatasetJsonConversion.datasetRequestToDataset(datasetRequest));
    }

    private UUID createDataset(String datasetFile) throws IOException {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
        return createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID().toString());
    }

    @Before
    public void setup() {
        billingProfile = ProfileFixtures.randomBillingProfile();
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);
    }

    @After
    public void teardown() {
        profileDao.deleteBillingProfileById(billingProfile.getId());
    }

    @Test(expected = DatasetNotFoundException.class)
    public void datasetDeleteTest() throws IOException {
        UUID datasetId = createDataset("dataset-minimal.json");
        assertThat("dataset delete signals success", datasetDao.delete(datasetId), equalTo(true));
        datasetDao.retrieve(datasetId);
    }

    @Test
    public void enumerateTest() throws Exception {
        UUID dataset1 = createDataset("dataset-minimal.json");
        UUID dataset2 = createDataset("dataset-create-test.json");
        List<UUID> datasetIds = new ArrayList<>();
        datasetIds.add(dataset1);
        datasetIds.add(dataset2);

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
        datasetDao.delete(dataset2);
    }

    @Test
    public void datasetTest() throws IOException {
        DatasetRequestModel request = jsonLoader.loadObject("dataset-create-test.json", DatasetRequestModel.class);
        String expectedName = request.getName() + UUID.randomUUID().toString();

        UUID datasetId = createDataset(request, expectedName);
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

        datasetDao.delete(datasetId);
    }

    @Test
    public void primaryKeyTest() throws IOException {
        UUID datasetId = createDataset("dataset-primary-key.json");
        Dataset fromDB = datasetDao.retrieve(datasetId);
        DatasetTable variants = fromDB.getTableByName("variant").orElseThrow(IllegalStateException::new);
        DatasetTable freqAnalysis = fromDB.getTableByName("frequency_analysis").orElseThrow(IllegalStateException::new);
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

        datasetDao.delete(datasetId);
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
