package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.metadata.Column;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.DatasetSummary;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import bio.terra.metadata.Table;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.service.DatasetService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

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
    private ObjectMapper objectMapper;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private StudyDao studyDao;

    @Autowired
    private DatasetService datasetService;

    private Study study;
    private UUID studyId;
    private DatasetRequestModel datasetRequest;
    private UUID datasetId;

    @Before
    public void setup() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJson = IOUtils.toString(classLoader.getResourceAsStream("dataset-test-study.json"));

        StudyRequestModel studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJson);
        studyRequest.setName(studyRequest.getName() + UUID.randomUUID().toString());
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        studyId = studyDao.create(study);
        study = studyDao.retrieve(studyId);

        String datasetJson = IOUtils.toString(classLoader.getResourceAsStream("dataset-test-dataset.json"));
        datasetRequest = objectMapper.readerFor(DatasetRequestModel.class).readValue(datasetJson);
        datasetRequest.getContents().get(0).getSource().setStudyName(study.getName());

        // Populate the datasetId with random; delete should quietly not find it.
        datasetId = UUID.randomUUID();
    }

    @After
    public void teardown() throws Exception {
        datasetDao.delete(datasetId);
        studyDao.delete(studyId);
    }

    @Test
    public void happyInOutTest() throws Exception {
        datasetRequest.name(datasetRequest.getName() + UUID.randomUUID().toString());

        Dataset dataset = datasetService.makeDatasetFromDatasetRequest(datasetRequest);
        datasetId = datasetDao.create(dataset);
        Dataset fromDB = datasetDao.retrieveDataset(datasetId);

        assertThat("dataset name set correctly",
                fromDB.getName(),
                equalTo(dataset.getName()));

        assertThat("dataset description set correctly",
                fromDB.getDescription(),
                equalTo(dataset.getDescription()));

        assertThat("correct number of tables created",
                fromDB.getTables().size(),
                equalTo(1));

        assertThat("correct number of sources created",
                fromDB.getDatasetSources().size(),
                equalTo(1));

        // verify source and map
        DatasetSource source = fromDB.getDatasetSources().get(0);
        assertThat("source points back to dataset",
                source.getDataset().getId(),
                equalTo(dataset.getId()));

        assertThat("source points to the asset spec",
                source.getAssetSpecification().getId(),
                equalTo(study.getAssetSpecifications().get(0).getId()));

        assertThat("correct number of map tables",
                source.getDatasetMapTables().size(),
                equalTo(1));

        // Verify map table
        DatasetMapTable mapTable = source.getDatasetMapTables().get(0);
        StudyTable studyTable = study.getTables().get(0);
        Table datasetTable = dataset.getTables().get(0);

        assertThat("correct map table study table",
                mapTable.getFromTable().getId(),
                equalTo(studyTable.getId()));

        assertThat("correct map table dataset table",
                mapTable.getToTable().getId(),
                equalTo(datasetTable.getId()));

        assertThat("correct number of map columns",
                mapTable.getDatasetMapColumns().size(),
                equalTo(1));

        // Verify map columns
        DatasetMapColumn mapColumn = mapTable.getDatasetMapColumns().get(0);
        // Why is study columns Collection and not List?
        StudyTableColumn studyColumn = studyTable.getColumns().iterator().next();
        Column datasetColumn = datasetTable.getColumns().get(0);

        assertThat("correct map column study column",
                mapColumn.getFromColumn().getId(),
                equalTo(studyColumn.getId()));

        assertThat("correct map column dataset column",
                mapColumn.getToColumn().getId(),
                equalTo(datasetColumn.getId()));
    }

    @Test
    public void datasetEnumerateTest() throws Exception {
        List<UUID> datasetIds = new ArrayList<>();
        String datasetName = datasetRequest.getName();

        // Make 6 datasets
        for (int i = 0; i < 6; i++) {
            datasetRequest.name(datasetName + i);
            Dataset dataset = datasetService.makeDatasetFromDatasetRequest(datasetRequest);
            datasetId = datasetDao.create(dataset);
            datasetIds.add(datasetId);
        }

        testOneEnumerateRange(datasetIds, datasetName, 0, 1000);
        testOneEnumerateRange(datasetIds, datasetName, 1, 3);
        testOneEnumerateRange(datasetIds, datasetName, 3, 5);
        testOneEnumerateRange(datasetIds, datasetName, 4, 7);
    }

    private void testOneEnumerateRange(List<UUID> datasetIds,
                                       String datasetName,
                                       int offset,
                                       int limit) {
        // We expect the datasets to be returned in their created order
        List<DatasetSummary> summaryList = datasetDao.retrieveDatasets(offset, limit);
        int index = offset;
        for (DatasetSummary summary : summaryList) {
            assertThat("correct dataset id",
                    datasetIds.get(index),
                    equalTo(summary.getId()));
            assertThat("correct dataset namee",
                    datasetName + index,
                    equalTo(summary.getName()));
            index++;
        }
    }




}
