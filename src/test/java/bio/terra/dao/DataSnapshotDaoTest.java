package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.metadata.Column;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotMapColumn;
import bio.terra.metadata.DataSnapshotMapTable;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.metadata.DataSnapshotSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.service.DataSnapshotService;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DataSnapshotDaoTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataSnapshotDao dataSnapshotDao;

    @Autowired
    private StudyDao studyDao;

    @Autowired
    private DataSnapshotService dataSnapshotService;

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
        dataSnapshotDao.delete(datasetId);
        studyDao.delete(studyId);
    }

    @Test
    public void happyInOutTest() throws Exception {
        datasetRequest.name(datasetRequest.getName() + UUID.randomUUID().toString());

        DataSnapshot dataSnapshot = dataSnapshotService.makeDatasetFromDatasetRequest(datasetRequest);
        datasetId = dataSnapshotDao.create(dataSnapshot);
        DataSnapshot fromDB = dataSnapshotDao.retrieveDataset(datasetId);

        assertThat("dataSnapshot name set correctly",
                fromDB.getName(),
                equalTo(dataSnapshot.getName()));

        assertThat("dataSnapshot description set correctly",
                fromDB.getDescription(),
                equalTo(dataSnapshot.getDescription()));

        assertThat("correct number of tables created",
                fromDB.getTables().size(),
                equalTo(1));

        assertThat("correct number of sources created",
                fromDB.getDataSnapshotSources().size(),
                equalTo(1));

        // verify source and map
        DataSnapshotSource source = fromDB.getDataSnapshotSources().get(0);
        assertThat("source points back to dataSnapshot",
                source.getDataSnapshot().getId(),
                equalTo(dataSnapshot.getId()));

        assertThat("source points to the asset spec",
                source.getAssetSpecification().getId(),
                equalTo(study.getAssetSpecifications().get(0).getId()));

        assertThat("correct number of map tables",
                source.getDataSnapshotMapTables().size(),
                equalTo(1));

        // Verify map table
        DataSnapshotMapTable mapTable = source.getDataSnapshotMapTables().get(0);
        Table studyTable = study.getTables().get(0);
        Table datasetTable = dataSnapshot.getTables().get(0);

        assertThat("correct map table study table",
                mapTable.getFromTable().getId(),
                equalTo(studyTable.getId()));

        assertThat("correct map table dataSnapshot table",
                mapTable.getToTable().getId(),
                equalTo(datasetTable.getId()));

        assertThat("correct number of map columns",
                mapTable.getDataSnapshotMapColumns().size(),
                equalTo(1));

        // Verify map columns
        DataSnapshotMapColumn mapColumn = mapTable.getDataSnapshotMapColumns().get(0);
        // Why is study columns Collection and not List?
        Column studyColumn = studyTable.getColumns().iterator().next();
        Column datasetColumn = datasetTable.getColumns().get(0);

        assertThat("correct map column study column",
                mapColumn.getFromColumn().getId(),
                equalTo(studyColumn.getId()));

        assertThat("correct map column dataSnapshot column",
                mapColumn.getToColumn().getId(),
                equalTo(datasetColumn.getId()));
    }

    @Test
    public void datasetEnumerateTest() throws Exception {
        // Delete all datasets from previous tests before we run this one so the results are predictable
        deleteAllDatasets();

        List<UUID> datasetIds = new ArrayList<>();
        String datasetName = datasetRequest.getName() + UUID.randomUUID().toString();

        for (int i = 0; i < 6; i++) {
            datasetRequest
                .name(makeName(datasetName, i))
                // set the description to a random string so we can verify the sorting is working independently of the
                // study name or created_date. add a suffix to filter on for the even datasets
                .description(UUID.randomUUID().toString() + ((i % 2 == 0) ? "==foo==" : ""));
            DataSnapshot dataSnapshot = dataSnapshotService.makeDatasetFromDatasetRequest(datasetRequest);
            datasetId = dataSnapshotDao.create(dataSnapshot);
            datasetIds.add(datasetId);
        }

        testOneEnumerateRange(datasetIds, datasetName, 0, 1000);
        testOneEnumerateRange(datasetIds, datasetName, 1, 3);
        testOneEnumerateRange(datasetIds, datasetName, 3, 5);
        testOneEnumerateRange(datasetIds, datasetName, 4, 7);

        testSortingNames(datasetIds, datasetName, 0, 10, "asc");
        testSortingNames(datasetIds, datasetName, 0, 3, "asc");
        testSortingNames(datasetIds, datasetName, 1, 3, "asc");
        testSortingNames(datasetIds, datasetName, 2, 5, "asc");
        testSortingNames(datasetIds, datasetName, 0, 10, "desc");
        testSortingNames(datasetIds, datasetName, 0, 3, "desc");
        testSortingNames(datasetIds, datasetName, 1, 3, "desc");
        testSortingNames(datasetIds, datasetName, 2, 5, "desc");

        testSortingDescriptions("desc");
        testSortingDescriptions("asc");


        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDatasets(0, 6, null,
            null, "==foo==");
        List<DataSnapshotSummary> summaryList = summaryEnum.getItems();
        assertThat("filtered 3 datasets", summaryList.size(), equalTo(3));
        assertThat("counts total 3", summaryEnum.getTotal(), equalTo(6));
        for (int i = 0; i < 3; i++) {
            assertThat("ids match", datasetIds.get(i * 2), equalTo(summaryList.get(i).getId()));
        }

        MetadataEnumeration<DataSnapshotSummary> emptyEnum = dataSnapshotDao.retrieveDatasets(0, 6, null,
            null, "__");
        assertThat("underscores don't act as wildcards", emptyEnum.getItems().size(), equalTo(0));

        for (UUID datasetId : datasetIds) {
            dataSnapshotDao.delete(datasetId);
        }
    }

    private String makeName(String baseName, int index) {
        return baseName + "-" + index;
    }

    private void testSortingNames(List<UUID> datasetIds, String datasetName, int offset, int limit, String direction) {
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDatasets(offset, limit, "name",
            direction, null);
        List<DataSnapshotSummary>  summaryList = summaryEnum.getItems();
        int index = (direction.equals("asc")) ? offset : datasetIds.size() - offset - 1;
        for (DataSnapshotSummary summary : summaryList) {
            assertThat("correct id", datasetIds.get(index), equalTo(summary.getId()));
            assertThat("correct name", makeName(datasetName, index), equalTo(summary.getName()));
            index += (direction.equals("asc")) ? 1 : -1;
        }
    }

    private void testSortingDescriptions(String direction) {
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDatasets(0, 6,
            "description", direction, null);
        List<DataSnapshotSummary> summaryList = summaryEnum.getItems();
        assertThat("the full list comes back", summaryList.size(), equalTo(6));
        String previous = summaryList.get(0).getDescription();
        for (int i = 1; i < summaryList.size(); i++) {
            String next = summaryList.get(i).getDescription();
            if (direction.equals("asc")) {
                assertThat("ascending order", previous, lessThan(next));
            } else {
                assertThat("descending order", previous, greaterThan(next));
            }
            previous = next;
        }

    }

    private void testOneEnumerateRange(List<UUID> datasetIds,
                                       String datasetName,
                                       int offset,
                                       int limit) {
        // We expect the datasets to be returned in their created order
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDatasets(offset, limit, "created_date",
            "asc", null);
        List<DataSnapshotSummary> summaryList = summaryEnum.getItems();
        int index = offset;
        for (DataSnapshotSummary summary : summaryList) {
            assertThat("correct dataset id",
                    datasetIds.get(index),
                    equalTo(summary.getId()));
            assertThat("correct dataset namee",
                    makeName(datasetName, index),
                    equalTo(summary.getName()));
            index++;
        }
    }

    private void deleteAllDatasets() {
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDatasets(0, 1000, null,
            null, null);
        List<DataSnapshotSummary> summaryList = summaryEnum.getItems();
        for (DataSnapshotSummary summary : summaryList) {
            dataSnapshotDao.delete(summary.getId());
        }
    }

}
