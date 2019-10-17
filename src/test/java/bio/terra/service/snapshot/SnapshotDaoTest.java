package bio.terra.service.snapshot;

import bio.terra.category.Unit;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.service.dataset.Dataset;
import bio.terra.common.Table;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.resourcemanagement.ProfileDao;
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
public class SnapshotDaoTest {

    @Autowired
    private SnapshotDao snapshotDao;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private SnapshotService snapshotService;

    @Autowired
    private JsonLoader jsonLoader;

    private Dataset dataset;
    private UUID datasetId;
    private SnapshotRequestModel snapshotRequest;
    private UUID snapshotId;
    private UUID profileId;

    @Before
    public void setup() throws Exception {
        profileId = profileDao.createBillingProfile(ProfileFixtures.randomBillingProfile());

        DatasetRequestModel datasetRequest = jsonLoader.loadObject("snapshot-test-dataset.json",
            DatasetRequestModel.class);
        datasetRequest
            .name(datasetRequest.getName() + UUID.randomUUID().toString())
            .defaultProfileId(profileId.toString());
        datasetId = datasetDao.create(DatasetJsonConversion.datasetRequestToDataset(datasetRequest));
        dataset = datasetDao.retrieve(datasetId);

        snapshotRequest = jsonLoader.loadObject("snapshot-test-snapshot.json", SnapshotRequestModel.class)
            .profileId(profileId.toString());
        snapshotRequest.getContents().get(0).getSource().setDatasetName(dataset.getName());

        // Populate the snapshotId with random; delete should quietly not find it.
        snapshotId = UUID.randomUUID();
    }

    @After
    public void teardown() throws Exception {
        snapshotDao.delete(snapshotId);
        datasetDao.delete(datasetId);
        profileDao.deleteBillingProfileById(profileId);
    }

    @Test
    public void happyInOutTest() throws Exception {
        snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID().toString());

        Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequest);
        snapshotId = snapshotDao.create(snapshot);
        Snapshot fromDB = snapshotDao.retrieveSnapshot(snapshotId);

        assertThat("snapshot name set correctly",
                fromDB.getName(),
                equalTo(snapshot.getName()));

        assertThat("snapshot description set correctly",
                fromDB.getDescription(),
                equalTo(snapshot.getDescription()));

        assertThat("correct number of tables created",
                fromDB.getTables().size(),
                equalTo(1));

        assertThat("correct number of sources created",
                fromDB.getSnapshotSources().size(),
                equalTo(1));

        // verify source and map
        SnapshotSource source = fromDB.getSnapshotSources().get(0);
        assertThat("source points back to snapshot",
                source.getSnapshot().getId(),
                equalTo(snapshot.getId()));

        assertThat("source points to the asset spec",
                source.getAssetSpecification().getId(),
                equalTo(dataset.getAssetSpecifications().get(0).getId()));

        assertThat("correct number of map tables",
                source.getSnapshotMapTables().size(),
                equalTo(1));

        // Verify map table
        SnapshotMapTable mapTable = source.getSnapshotMapTables().get(0);
        Table datasetTable = dataset.getTables().get(0);
        Table snapshotTable = snapshot.getTables().get(0);

        assertThat("correct map table dataset table",
                mapTable.getFromTable().getId(),
                equalTo(datasetTable.getId()));

        assertThat("correct map table snapshot table",
                mapTable.getToTable().getId(),
                equalTo(snapshotTable.getId()));

        assertThat("correct number of map columns",
                mapTable.getSnapshotMapColumns().size(),
                equalTo(1));

        // Verify map columns
        SnapshotMapColumn mapColumn = mapTable.getSnapshotMapColumns().get(0);
        // Why is dataset columns Collection and not List?
        Column datasetColumn = datasetTable.getColumns().iterator().next();
        Column snapshotColumn = snapshotTable.getColumns().get(0);

        assertThat("correct map column dataset column",
                mapColumn.getFromColumn().getId(),
                equalTo(datasetColumn.getId()));

        assertThat("correct map column snapshot column",
                mapColumn.getToColumn().getId(),
                equalTo(snapshotColumn.getId()));
    }

    @Test
    public void snapshotEnumerateTest() throws Exception {
        List<UUID> snapshotIds = new ArrayList<>();
        String snapshotName = snapshotRequest.getName() + UUID.randomUUID().toString();

        for (int i = 0; i < 6; i++) {
            snapshotRequest
                .name(makeName(snapshotName, i))
                // set the description to a random string so we can verify the sorting is working independently of the
                // dataset name or created_date. add a suffix to filter on for the even snapshots
                .description(UUID.randomUUID().toString() + ((i % 2 == 0) ? "==foo==" : ""));
            Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequest);
            snapshotId = snapshotDao.create(snapshot);
            snapshotIds.add(snapshotId);
        }

        testOneEnumerateRange(snapshotIds, snapshotName, 0, 1000);
        testOneEnumerateRange(snapshotIds, snapshotName, 1, 3);
        testOneEnumerateRange(snapshotIds, snapshotName, 3, 5);
        testOneEnumerateRange(snapshotIds, snapshotName, 4, 7);

        testSortingNames(snapshotIds, snapshotName, 0, 10, "asc");
        testSortingNames(snapshotIds, snapshotName, 0, 3, "asc");
        testSortingNames(snapshotIds, snapshotName, 1, 3, "asc");
        testSortingNames(snapshotIds, snapshotName, 2, 5, "asc");
        testSortingNames(snapshotIds, snapshotName, 0, 10, "desc");
        testSortingNames(snapshotIds, snapshotName, 0, 3, "desc");
        testSortingNames(snapshotIds, snapshotName, 1, 3, "desc");
        testSortingNames(snapshotIds, snapshotName, 2, 5, "desc");

        testSortingDescriptions(snapshotIds, "desc");
        testSortingDescriptions(snapshotIds, "asc");


        MetadataEnumeration<SnapshotSummary> summaryEnum = snapshotDao.retrieveSnapshots(0, 2, null,
            null, "==foo==", snapshotIds);
        List<SnapshotSummary> summaryList = summaryEnum.getItems();
        assertThat("filtered and retrieved 2 snapshots", summaryList.size(), equalTo(2));
        assertThat("filtered total 3", summaryEnum.getTotal(), equalTo(3));
        for (int i = 0; i < 2; i++) {
            assertThat("first 2 ids match", snapshotIds.get(i * 2), equalTo(summaryList.get(i).getId()));
        }

        MetadataEnumeration<SnapshotSummary> emptyEnum = snapshotDao.retrieveSnapshots(0, 6, null,
            null, "__", snapshotIds);
        assertThat("underscores don't act as wildcards", emptyEnum.getItems().size(), equalTo(0));

        for (UUID snapshotId : snapshotIds) {
            snapshotDao.delete(snapshotId);
        }
    }

    private String makeName(String baseName, int index) {
        return baseName + "-" + index;
    }

    private void testSortingNames(
        List<UUID> snapshotIds,
        String snapshotName,
        int offset,
        int limit,
        String direction) {
        MetadataEnumeration<SnapshotSummary> summaryEnum = snapshotDao.retrieveSnapshots(offset, limit, "name",
            direction, null, snapshotIds);
        List<SnapshotSummary>  summaryList = summaryEnum.getItems();
        int index = (direction.equals("asc")) ? offset : snapshotIds.size() - offset - 1;
        for (SnapshotSummary summary : summaryList) {
            assertThat("correct id", snapshotIds.get(index), equalTo(summary.getId()));
            assertThat("correct name", makeName(snapshotName, index), equalTo(summary.getName()));
            index += (direction.equals("asc")) ? 1 : -1;
        }
    }

    private void testSortingDescriptions(List<UUID> snapshotIds, String direction) {
        MetadataEnumeration<SnapshotSummary> summaryEnum = snapshotDao.retrieveSnapshots(0, 6,
            "description", direction, null, snapshotIds);
        List<SnapshotSummary> summaryList = summaryEnum.getItems();
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

    private void testOneEnumerateRange(List<UUID> snapshotIds,
                                       String snapshotName,
                                       int offset,
                                       int limit) {
        // We expect the snapshots to be returned in their created order
        MetadataEnumeration<SnapshotSummary> summaryEnum = snapshotDao.retrieveSnapshots(offset, limit, "created_date",
            "asc", null, snapshotIds);
        List<SnapshotSummary> summaryList = summaryEnum.getItems();
        int index = offset;
        for (SnapshotSummary summary : summaryList) {
            assertThat("correct snapshot id",
                    snapshotIds.get(index),
                    equalTo(summary.getId()));
            assertThat("correct snapshot namee",
                    makeName(snapshotName, index),
                    equalTo(summary.getName()));
            index++;
        }
    }
}
