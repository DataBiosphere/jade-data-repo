package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.dao.exception.DrDatasetNotFoundException;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.DrDatasetSummary;
import bio.terra.metadata.Table;
import bio.terra.model.DrDatasetJsonConversion;
import bio.terra.model.DrDatasetRequestModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
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
public class DrDatasetDaoTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DrDatasetDao datasetDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private DrDataset dataset;
    private UUID datasetId;
    private DrDataset fromDB;
    private boolean deleted;

    @Before
    public void setup() throws Exception {
        DrDatasetRequestModel datasetRequest = getDatasetRequestModel("dataset-create-test.json");
        datasetRequest.setName(datasetRequest.getName() + UUID.randomUUID().toString());
        dataset = DrDatasetJsonConversion.datasetRequestToDataset(datasetRequest);
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
    }

    private DrDatasetRequestModel getDatasetRequestModel(String jsonResourceFileName) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        String datasetJsonStr = IOUtils.toString(classLoader.getResourceAsStream(jsonResourceFileName));
        return objectMapper.readerFor(DrDatasetRequestModel.class).readValue(datasetJsonStr);
    }

    private UUID createMinimalDataset() throws IOException {
        DrDatasetRequestModel datasetRequest = getDatasetRequestModel("dataset-minimal.json");
        datasetRequest.setName(datasetRequest.getName() + UUID.randomUUID().toString());
        return datasetDao.create(DrDatasetJsonConversion.datasetRequestToDataset(datasetRequest));
    }

    @Test(expected = DrDatasetNotFoundException.class)
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

        MetadataEnumeration<DrDatasetSummary> summaryEnum = datasetDao.enumerate(0, 2, "created_date",
            "asc", null, datasetIds);
        List<DrDatasetSummary> datasets = summaryEnum.getItems();
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

    protected void assertTablesInRelationship(DrDataset dataset) {
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
