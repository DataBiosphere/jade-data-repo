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
import bio.terra.model.DataSnapshotRequestModel;
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
    private DataSnapshotRequestModel dataSnapshotRequest;
    private UUID dataSnapshotId;

    @Before
    public void setup() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJson = IOUtils.toString(classLoader.getResourceAsStream("datasnapshot-test-study.json"));

        StudyRequestModel studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJson);
        studyRequest.setName(studyRequest.getName() + UUID.randomUUID().toString());
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        studyId = studyDao.create(study);
        study = studyDao.retrieve(studyId);

        String dataSnapshotJson = IOUtils.toString(classLoader.getResourceAsStream("datasnapshot-test.json"));
        dataSnapshotRequest = objectMapper.readerFor(DataSnapshotRequestModel.class).readValue(dataSnapshotJson);
        dataSnapshotRequest.getContents().get(0).getSource().setStudyName(study.getName());

        // Populate the dataSnapshotId with random; delete should quietly not find it.
        dataSnapshotId = UUID.randomUUID();
    }

    @After
    public void teardown() throws Exception {
        dataSnapshotDao.delete(dataSnapshotId);
        studyDao.delete(studyId);
    }

    @Test
    public void happyInOutTest() throws Exception {
        dataSnapshotRequest.name(dataSnapshotRequest.getName() + UUID.randomUUID().toString());

        DataSnapshot dataSnapshot = dataSnapshotService.makeDataSnapshotFromDataSnapshotRequest(dataSnapshotRequest);
        dataSnapshotId = dataSnapshotDao.create(dataSnapshot);
        DataSnapshot fromDB = dataSnapshotDao.retrieveDataSnapshot(dataSnapshotId);

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
        Table dataSnapshotTable = dataSnapshot.getTables().get(0);

        assertThat("correct map table study table",
                mapTable.getFromTable().getId(),
                equalTo(studyTable.getId()));

        assertThat("correct map table dataSnapshot table",
                mapTable.getToTable().getId(),
                equalTo(dataSnapshotTable.getId()));

        assertThat("correct number of map columns",
                mapTable.getDataSnapshotMapColumns().size(),
                equalTo(1));

        // Verify map columns
        DataSnapshotMapColumn mapColumn = mapTable.getDataSnapshotMapColumns().get(0);
        // Why is study columns Collection and not List?
        Column studyColumn = studyTable.getColumns().iterator().next();
        Column dataSnapshotColumn = dataSnapshotTable.getColumns().get(0);

        assertThat("correct map column study column",
                mapColumn.getFromColumn().getId(),
                equalTo(studyColumn.getId()));

        assertThat("correct map column dataSnapshot column",
                mapColumn.getToColumn().getId(),
                equalTo(dataSnapshotColumn.getId()));
    }

    @Test
    public void dataSnapshotEnumerateTest() throws Exception {
        // Delete all dataSnapshots from previous tests before we run this one so the results are predictable
        deleteAllDataSnapshots();

        List<UUID> dataSnapshotIds = new ArrayList<>();
        String dataSnapshotName = dataSnapshotRequest.getName() + UUID.randomUUID().toString();

        for (int i = 0; i < 6; i++) {
            dataSnapshotRequest
                .name(makeName(dataSnapshotName, i))
                // set the description to a random string so we can verify the sorting is working independently of the
                // study name or created_date. add a suffix to filter on for the even dataSnapshots
                .description(UUID.randomUUID().toString() + ((i % 2 == 0) ? "==foo==" : ""));
            DataSnapshot dataSnapshot = dataSnapshotService.makeDataSnapshotFromDataSnapshotRequest(dataSnapshotRequest);
            dataSnapshotId = dataSnapshotDao.create(dataSnapshot);
            dataSnapshotIds.add(dataSnapshotId);
        }

        testOneEnumerateRange(dataSnapshotIds, dataSnapshotName, 0, 1000);
        testOneEnumerateRange(dataSnapshotIds, dataSnapshotName, 1, 3);
        testOneEnumerateRange(dataSnapshotIds, dataSnapshotName, 3, 5);
        testOneEnumerateRange(dataSnapshotIds, dataSnapshotName, 4, 7);

        testSortingNames(dataSnapshotIds, dataSnapshotName, 0, 10, "asc");
        testSortingNames(dataSnapshotIds, dataSnapshotName, 0, 3, "asc");
        testSortingNames(dataSnapshotIds, dataSnapshotName, 1, 3, "asc");
        testSortingNames(dataSnapshotIds, dataSnapshotName, 2, 5, "asc");
        testSortingNames(dataSnapshotIds, dataSnapshotName, 0, 10, "desc");
        testSortingNames(dataSnapshotIds, dataSnapshotName, 0, 3, "desc");
        testSortingNames(dataSnapshotIds, dataSnapshotName, 1, 3, "desc");
        testSortingNames(dataSnapshotIds, dataSnapshotName, 2, 5, "desc");

        testSortingDescriptions("desc");
        testSortingDescriptions("asc");


        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDataSnapshots(0, 6, null,
            null, "==foo==");
        List<DataSnapshotSummary> summaryList = summaryEnum.getItems();
        assertThat("filtered 3 dataSnapshots", summaryList.size(), equalTo(3));
        assertThat("counts total 3", summaryEnum.getTotal(), equalTo(6));
        for (int i = 0; i < 3; i++) {
            assertThat("ids match", dataSnapshotIds.get(i * 2), equalTo(summaryList.get(i).getId()));
        }

        MetadataEnumeration<DataSnapshotSummary> emptyEnum = dataSnapshotDao.retrieveDataSnapshots(0, 6, null,
            null, "__");
        assertThat("underscores don't act as wildcards", emptyEnum.getItems().size(), equalTo(0));

        for (UUID dataSnapshotId : dataSnapshotIds) {
            dataSnapshotDao.delete(dataSnapshotId);
        }
    }

    private String makeName(String baseName, int index) {
        return baseName + "-" + index;
    }

    private void testSortingNames(List<UUID> dataSnapshotIds, String dataSnapshotName, int offset, int limit, String direction) {
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDataSnapshots(offset, limit, "name",
            direction, null);
        List<DataSnapshotSummary>  summaryList = summaryEnum.getItems();
        int index = (direction.equals("asc")) ? offset : dataSnapshotIds.size() - offset - 1;
        for (DataSnapshotSummary summary : summaryList) {
            assertThat("correct id", dataSnapshotIds.get(index), equalTo(summary.getId()));
            assertThat("correct name", makeName(dataSnapshotName, index), equalTo(summary.getName()));
            index += (direction.equals("asc")) ? 1 : -1;
        }
    }

    private void testSortingDescriptions(String direction) {
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDataSnapshots(0, 6,
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

    private void testOneEnumerateRange(List<UUID> dataSnapshotIds,
                                       String dataSnapshotName,
                                       int offset,
                                       int limit) {
        // We expect the dataSnapshots to be returned in their created order
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDataSnapshots(offset, limit, "created_date",
            "asc", null);
        List<DataSnapshotSummary> summaryList = summaryEnum.getItems();
        int index = offset;
        for (DataSnapshotSummary summary : summaryList) {
            assertThat("correct dataSnapshot id",
                    dataSnapshotIds.get(index),
                    equalTo(summary.getId()));
            assertThat("correct dataSnapshot name",
                    makeName(dataSnapshotName, index),
                    equalTo(summary.getName()));
            index++;
        }
    }

    private void deleteAllDataSnapshots() {
        MetadataEnumeration<DataSnapshotSummary> summaryEnum = dataSnapshotDao.retrieveDataSnapshots(0, 1000, null,
            null, null);
        List<DataSnapshotSummary> summaryList = summaryEnum.getItems();
        for (DataSnapshotSummary summary : summaryList) {
            dataSnapshotDao.delete(summary.getId());
        }
    }

}
