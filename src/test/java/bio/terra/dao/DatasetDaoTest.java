package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.ProfileFixtures;
import bio.terra.metadata.Column;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.DatasetSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.resourcemanagement.dao.ProfileDao;
import bio.terra.service.DatasetService;
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
public class DatasetDaoTest {

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private StudyDao studyDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private JsonLoader jsonLoader;

    private Study study;
    private UUID studyId;
    private DatasetRequestModel datasetRequest;
    private UUID datasetId;
    private UUID profileId;

    @Before
    public void setup() throws Exception {
        profileId = profileDao.createBillingProfile(ProfileFixtures.randomBillingProfile());

        StudyRequestModel studyRequest = jsonLoader.loadObject("dataset-test-study.json",
            StudyRequestModel.class);
        studyRequest
            .name(studyRequest.getName() + UUID.randomUUID().toString())
            .defaultProfileId(profileId.toString());
        studyId = studyDao.create(StudyJsonConversion.studyRequestToStudy(studyRequest));
        study = studyDao.retrieve(studyId);

        datasetRequest = jsonLoader.loadObject("dataset-test-dataset.json", DatasetRequestModel.class)
            .profileId(profileId.toString());
        datasetRequest.getContents().get(0).getSource().setStudyName(study.getName());

        // Populate the datasetId with random; delete should quietly not find it.
        datasetId = UUID.randomUUID();
    }

    @After
    public void teardown() throws Exception {
        datasetDao.delete(datasetId);
        studyDao.delete(studyId);
        profileDao.deleteBillingProfileById(profileId);
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
        Table studyTable = study.getTables().get(0);
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
        Column studyColumn = studyTable.getColumns().iterator().next();
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
            Dataset dataset = datasetService.makeDatasetFromDatasetRequest(datasetRequest);
            datasetId = datasetDao.create(dataset);
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


        MetadataEnumeration<DatasetSummary> summaryEnum = datasetDao.retrieveDatasets(0, 6, null,
            null, "==foo==");
        List<DatasetSummary> summaryList = summaryEnum.getItems();
        assertThat("filtered 3 datasets", summaryList.size(), equalTo(3));
        assertThat("counts total 3", summaryEnum.getTotal(), equalTo(6));
        for (int i = 0; i < 3; i++) {
            assertThat("ids match", datasetIds.get(i * 2), equalTo(summaryList.get(i).getId()));
        }

        MetadataEnumeration<DatasetSummary> emptyEnum = datasetDao.retrieveDatasets(0, 6, null,
            null, "__");
        assertThat("underscores don't act as wildcards", emptyEnum.getItems().size(), equalTo(0));

        for (UUID datasetId : datasetIds) {
            datasetDao.delete(datasetId);
        }
    }

    private String makeName(String baseName, int index) {
        return baseName + "-" + index;
    }

    private void testSortingNames(List<UUID> datasetIds, String datasetName, int offset, int limit, String direction) {
        MetadataEnumeration<DatasetSummary> summaryEnum = datasetDao.retrieveDatasets(offset, limit, "name",
            direction, null);
        List<DatasetSummary>  summaryList = summaryEnum.getItems();
        int index = (direction.equals("asc")) ? offset : datasetIds.size() - offset - 1;
        for (DatasetSummary summary : summaryList) {
            assertThat("correct id", datasetIds.get(index), equalTo(summary.getId()));
            assertThat("correct name", makeName(datasetName, index), equalTo(summary.getName()));
            index += (direction.equals("asc")) ? 1 : -1;
        }
    }

    private void testSortingDescriptions(String direction) {
        MetadataEnumeration<DatasetSummary> summaryEnum = datasetDao.retrieveDatasets(0, 6,
            "description", direction, null);
        List<DatasetSummary> summaryList = summaryEnum.getItems();
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
        MetadataEnumeration<DatasetSummary> summaryEnum = datasetDao.retrieveDatasets(offset, limit, "created_date",
            "asc", null);
        List<DatasetSummary> summaryList = summaryEnum.getItems();
        int index = offset;
        for (DatasetSummary summary : summaryList) {
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
        MetadataEnumeration<DatasetSummary> summaryEnum = datasetDao.retrieveDatasets(0, 1000, null,
            null, null);
        List<DatasetSummary> summaryList = summaryEnum.getItems();
        for (DatasetSummary summary : summaryList) {
            datasetDao.delete(summary.getId());
        }
    }

}
