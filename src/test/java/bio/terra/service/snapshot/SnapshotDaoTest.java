package bio.terra.service.snapshot;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;

import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.dataset.StorageResource;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.snapshot.exception.MissingRowCountsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class SnapshotDaoTest {

  @Autowired private SnapshotDao snapshotDao;

  @Autowired private DatasetDao datasetDao;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private SnapshotService snapshotService;

  @Autowired private JsonLoader jsonLoader;

  private Dataset dataset;
  private UUID datasetId;
  private SnapshotRequestModel snapshotRequest;
  private UUID snapshotId;
  private List<UUID> snapshotIdList;
  private List<UUID> datasetIds;
  private UUID profileId;
  private UUID projectId;

  @Before
  public void setup() throws Exception {
    BillingProfileModel billingProfile =
        profileDao.createBillingProfile(ProfileFixtures.randomBillingProfileRequest(), "hi@hi.hi");
    profileId = billingProfile.getId();

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    projectId = resourceDao.createProject(projectResource);

    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(
            "snapshot-test-dataset-with-multi-columns.json", DatasetRequestModel.class);
    datasetRequest
        .name(datasetRequest.getName() + UUID.randomUUID())
        .defaultProfileId(profileId)
        .cloudPlatform(CloudPlatform.GCP);

    dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectId);

    String createFlightId = UUID.randomUUID().toString();
    datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);
    dataset = datasetDao.retrieve(datasetId);

    snapshotRequest =
        jsonLoader
            .loadObject("snapshot-test-snapshot.json", SnapshotRequestModel.class)
            .profileId(profileId);
    snapshotRequest.getContents().get(0).setDatasetName(dataset.getName());

    // Populate the snapshotId with random; delete should quietly not find it.
    snapshotId = UUID.randomUUID();
    datasetIds = new ArrayList<>();
  }

  @After
  public void teardown() throws Exception {
    if (snapshotIdList != null) {
      for (UUID id : snapshotIdList) {
        snapshotDao.delete(id);
      }
      snapshotIdList = null;
    }
    snapshotDao.delete(snapshotId);
    datasetDao.delete(datasetId);
    resourceDao.deleteProject(projectId);
    profileDao.deleteBillingProfileById(profileId);
  }

  @Test(expected = MissingRowCountsException.class)
  public void testMissingRowCounts() throws Exception {
    Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequest);
    snapshot.projectResourceId(projectId);
    snapshotDao.updateSnapshotTableRowCounts(snapshot, Collections.emptyMap());
  }

  private Snapshot insertAndRetrieveSnapshot(Snapshot snapshot, String flightId) {
    snapshotDao.createAndLock(snapshot, flightId);
    snapshotDao.unlock(snapshotId, flightId);
    return snapshotDao.retrieveSnapshot(snapshotId);
  }

  @Test
  public void happyInOutTest() {
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());

    Snapshot snapshot =
        snapshotService
            .makeSnapshotFromSnapshotRequest(snapshotRequest)
            .projectResourceId(projectId)
            .id(snapshotId);
    Snapshot fromDB = insertAndRetrieveSnapshot(snapshot, "happyInOutTest_flightId");
    assertThat("snapshot name set correctly", fromDB.getName(), equalTo(snapshot.getName()));

    assertThat(
        "snapshot description set correctly",
        fromDB.getDescription(),
        equalTo(snapshot.getDescription()));

    assertThat("correct number of tables created", fromDB.getTables().size(), equalTo(2));

    assertThat("correct number of sources created", fromDB.getSnapshotSources().size(), equalTo(1));

    assertThat(
        "snapshot creation information set correctly",
        fromDB.getCreationInformation(),
        equalTo(snapshot.getCreationInformation()));

    // verify source and map
    SnapshotSource source = fromDB.getFirstSnapshotSource();
    assertThat(
        "source points back to snapshot", source.getSnapshot().getId(), equalTo(snapshot.getId()));

    // verify snapshot source region includes the default region
    assertTrue(
        "source dataset info includes default region",
        GoogleRegion.matchingRegionWithFallbacks(
            source.getDataset().getDatasetSummary().getStorage(),
            GoogleRegion.DEFAULT_GOOGLE_REGION));

    assertThat(
        "source points to the asset spec",
        source.getAssetSpecification().getId(),
        equalTo(dataset.getAssetSpecifications().get(0).getId()));

    assertThat("correct number of map tables", source.getSnapshotMapTables().size(), equalTo(2));

    // Verify map table
    SnapshotMapTable mapTable =
        source.getSnapshotMapTables().stream()
            .filter(t -> t.getFromTable().getName().equals("thetable"))
            .findFirst()
            .orElseThrow(AssertionError::new);
    Table datasetTable =
        dataset.getTables().stream()
            .filter(t -> t.getName().equals("thetable"))
            .findFirst()
            .orElseThrow(AssertionError::new);
    Table snapshotTable1 =
        snapshot.getTables().stream()
            .filter(t -> t.getName().equals("thetable"))
            .findFirst()
            .orElseThrow(AssertionError::new);
    Table snapshotTable2 =
        snapshot.getTables().stream()
            .filter(t -> t.getName().equals("anothertable"))
            .findFirst()
            .orElseThrow(AssertionError::new);

    assertThat(
        "correct map table dataset table",
        mapTable.getFromTable().getId(),
        equalTo(datasetTable.getId()));

    assertThat(
        "correct map table snapshot table",
        mapTable.getToTable().getId(),
        equalTo(snapshotTable1.getId()));

    assertThat(
        "correct number of map columns", mapTable.getSnapshotMapColumns().size(), equalTo(3));

    // Verify map columns
    SnapshotMapColumn mapColumn = mapTable.getSnapshotMapColumns().get(0);
    Column datasetColumn = datasetTable.getColumns().iterator().next();
    Column snapshotColumn = snapshotTable1.getColumns().get(0);

    assertThat(
        "correct map column dataset column",
        mapColumn.getFromColumn().getId(),
        equalTo(datasetColumn.getId()));

    assertThat(
        "correct map column snapshot column",
        mapColumn.getToColumn().getId(),
        equalTo(snapshotColumn.getId()));

    List<Relationship> relationships = fromDB.getRelationships();
    assertThat("a relationship comes back", relationships.size(), equalTo(1));
    Relationship relationship = relationships.get(0);
    Table fromTable = relationship.getFromTable();
    Column fromColumn = relationship.getFromColumn();
    Table toTable = relationship.getToTable();
    Column toColumn = relationship.getToColumn();
    assertThat("from table name matches", fromTable.getName(), equalTo("thetable"));
    assertThat("from column name matches", fromColumn.getName(), equalTo("thecolumn1"));
    assertThat("to table name matches", toTable.getName(), equalTo("anothertable"));
    assertThat("to column name matches", toColumn.getName(), equalTo("anothercolumn1"));
    assertThat(
        "relationship points to the snapshot table",
        fromTable.getId(),
        equalTo(snapshotTable1.getId()));

    // Verify snapshot column orders
    assertThat(
        "First table columns are in ascending order of name",
        snapshotTable1.getColumns().stream().map(Column::getName).collect(Collectors.toList()),
        contains("thecolumn1", "thecolumn2", "thecolumn3"));

    assertThat(
        "Second table columns are in descending order of name",
        snapshotTable2.getColumns().stream().map(Column::getName).collect(Collectors.toList()),
        contains("anothercolumn3", "anothercolumn2", "anothercolumn1"));
  }

  @Test
  public void snapshotEnumerateTest() throws Exception {
    snapshotIdList = new ArrayList<>();
    String snapshotName = snapshotRequest.getName() + UUID.randomUUID().toString();

    for (int i = 0; i < 6; i++) {
      snapshotRequest
          .name(makeName(snapshotName, i))
          // set the description to a random string so we can verify the sorting is working
          // independently of the
          // dataset name or created_date. add a suffix to filter on for the even snapshots
          .description(UUID.randomUUID().toString() + ((i % 2 == 0) ? "==foo==" : ""));
      String flightId = "snapshotEnumerateTest_flightId";
      UUID tempSnapshotId = UUID.randomUUID();
      Snapshot snapshot =
          snapshotService
              .makeSnapshotFromSnapshotRequest(snapshotRequest)
              .projectResourceId(projectId)
              .id(tempSnapshotId);
      snapshotDao.createAndLock(snapshot, flightId);

      snapshotDao.unlock(tempSnapshotId, flightId);
      snapshotIdList.add(tempSnapshotId);
    }
    MetadataEnumeration<SnapshotSummary> allSnapshots =
        snapshotDao.retrieveSnapshots(0, 6, null, null, null, null, datasetIds, snapshotIdList);

    for (var snapshotSummary : allSnapshots.getItems()) {
      assertThat(
          "snapshot summary has the GCP cloud platform",
          snapshotSummary.getCloudPlatform(),
          equalTo(CloudPlatform.GCP));
      assertThat(
          "snapshot summary has a data project", snapshotSummary.getDataProject(), notNullValue());
    }

    testOneEnumerateRange(snapshotIdList, snapshotName, 0, 1000);
    testOneEnumerateRange(snapshotIdList, snapshotName, 1, 3);
    testOneEnumerateRange(snapshotIdList, snapshotName, 3, 5);
    testOneEnumerateRange(snapshotIdList, snapshotName, 4, 7);

    testSortingNames(snapshotIdList, snapshotName, 0, 10, SqlSortDirection.ASC);
    testSortingNames(snapshotIdList, snapshotName, 0, 3, SqlSortDirection.ASC);
    testSortingNames(snapshotIdList, snapshotName, 1, 3, SqlSortDirection.ASC);
    testSortingNames(snapshotIdList, snapshotName, 2, 5, SqlSortDirection.ASC);
    testSortingNames(snapshotIdList, snapshotName, 0, 10, SqlSortDirection.DESC);
    testSortingNames(snapshotIdList, snapshotName, 0, 3, SqlSortDirection.DESC);
    testSortingNames(snapshotIdList, snapshotName, 1, 3, SqlSortDirection.DESC);
    testSortingNames(snapshotIdList, snapshotName, 2, 5, SqlSortDirection.DESC);

    testSortingDescriptions(snapshotIdList, SqlSortDirection.DESC);
    testSortingDescriptions(snapshotIdList, SqlSortDirection.ASC);

    MetadataEnumeration<SnapshotSummary> filterDefaultRegionEnum =
        snapshotDao.retrieveSnapshots(
            0,
            6,
            null,
            null,
            null,
            GoogleRegion.DEFAULT_GOOGLE_REGION.toString(),
            datasetIds,
            snapshotIdList);
    List<SnapshotSummary> filteredRegionSnapshots = filterDefaultRegionEnum.getItems();
    assertThat(
        "snapshot filter by default GCS region returns correct total",
        filteredRegionSnapshots.size(),
        equalTo(snapshotIdList.size()));
    for (SnapshotSummary s : filteredRegionSnapshots) {
      Snapshot snapshot = snapshotDao.retrieveSnapshot(s.getId());
      assertTrue(
          "snapshot filter by default GCS region returns correct items",
          snapshot
              .getFirstSnapshotSource()
              .getDataset()
              .getDatasetSummary()
              .datasetStorageContainsRegion(GoogleRegion.DEFAULT_GOOGLE_REGION));
    }

    MetadataEnumeration<SnapshotSummary> filterNameAndRegionEnum =
        snapshotDao.retrieveSnapshots(
            0,
            6,
            null,
            null,
            makeName(snapshotName, 0),
            GoogleRegion.DEFAULT_GOOGLE_REGION.toString(),
            datasetIds,
            snapshotIdList);
    List<SnapshotSummary> filteredNameAndRegionSnapshots = filterNameAndRegionEnum.getItems();
    assertThat(
        "snapshot filter by name and region returns correct total",
        filteredNameAndRegionSnapshots.size(),
        equalTo(1));
    assertThat(
        "snapshot filter by name and region returns correct snapshot name",
        filteredNameAndRegionSnapshots.get(0).getName(),
        equalTo(makeName(snapshotName, 0)));
    for (SnapshotSummary s : filteredNameAndRegionSnapshots) {
      Snapshot snapshot = snapshotDao.retrieveSnapshot(s.getId());
      assertTrue(
          "snapshot filter by name and region returns correct snapshot source region",
          snapshot
              .getFirstSnapshotSource()
              .getDataset()
              .getDatasetSummary()
              .datasetStorageContainsRegion(GoogleRegion.DEFAULT_GOOGLE_REGION));

      // Test retrieve SnapshotProject object
      SnapshotProject snapshotProject = snapshotDao.retrieveSnapshotProject(s.getId(), true);
      assertThat("snapshot project id matches", snapshotProject.getId(), equalTo(s.getId()));
      assertThat("snapshot project name matches", snapshotProject.getName(), equalTo(s.getName()));
      assertThat(
          "snapshot project profile id matches",
          snapshotProject.getProfileId(),
          equalTo(snapshot.getProfileId()));
      assertThat(
          "snapshot project data project name matches",
          snapshotProject.getDataProject(),
          equalTo(snapshot.getProjectResource().getGoogleProjectId()));
      assertThat(
          "snapshot project has a single source dataset",
          snapshotProject.getSourceDatasetProjects().size(),
          equalTo(1));
      assertThat(
          "dataset project id matches",
          snapshotProject.getFirstSourceDatasetProject().getId(),
          equalTo(snapshot.getSourceDataset().getId()));
      assertThat(
          "dataset project name matches",
          snapshotProject.getFirstSourceDatasetProject().getName(),
          equalTo(snapshot.getSourceDataset().getName()));
      assertThat(
          "dataset project profile id matches",
          snapshotProject.getFirstSourceDatasetProject().getProfileId(),
          equalTo(snapshot.getSourceDataset().getProjectResource().getProfileId()));
      assertThat(
          "dataset project data project name matches",
          snapshotProject.getFirstSourceDatasetProject().getDataProject(),
          equalTo(
              snapshot
                  .getFirstSnapshotSource()
                  .getDataset()
                  .getProjectResource()
                  .getGoogleProjectId()));
    }

    MetadataEnumeration<SnapshotSummary> summaryEnum =
        snapshotDao.retrieveSnapshots(
            0,
            2,
            EnumerateSortByParam.CREATED_DATE,
            SqlSortDirection.ASC,
            "==foo==",
            null,
            datasetIds,
            snapshotIdList);
    List<SnapshotSummary> summaryList = summaryEnum.getItems();
    assertThat("filtered and retrieved 2 snapshots", summaryList.size(), equalTo(2));
    assertThat("filtered total 3", summaryEnum.getFilteredTotal(), equalTo(3));
    assertThat("total 6", summaryEnum.getTotal(), equalTo(6));
    for (int i = 0; i < 2; i++) {
      assertThat(
          "first 2 ids match", snapshotIdList.get(i * 2), equalTo(summaryList.get(i).getId()));
    }

    MetadataEnumeration<SnapshotSummary> emptyEnum =
        snapshotDao.retrieveSnapshots(0, 6, null, null, "__", null, datasetIds, snapshotIdList);
    assertThat("underscores don't act as wildcards", emptyEnum.getItems().size(), equalTo(0));

    MetadataEnumeration<SnapshotSummary> summaryEnum0 =
        snapshotDao.retrieveSnapshots(0, 10, null, null, null, null, datasetIds, snapshotIdList);
    assertThat("no dataset uuid gives all snapshots", summaryEnum0.getTotal(), equalTo(6));

    // use the original dataset id and make sure you get all snapshots
    datasetIds = singletonList(datasetId);
    MetadataEnumeration<SnapshotSummary> summaryEnum1 =
        snapshotDao.retrieveSnapshots(0, 10, null, null, null, null, datasetIds, snapshotIdList);
    assertThat(
        "expected dataset uuid gives expected snapshot", summaryEnum1.getTotal(), equalTo(6));

    // made a random dataset uuid and made sure that you get no snapshots
    List<UUID> datasetIdsBad = singletonList(UUID.randomUUID());
    MetadataEnumeration<SnapshotSummary> summaryEnum2 =
        snapshotDao.retrieveSnapshots(0, 10, null, null, null, null, datasetIdsBad, snapshotIdList);
    assertThat("dummy dataset uuid gives no snapshots", summaryEnum2.getTotal(), equalTo(0));
  }

  private String makeName(String baseName, int index) {
    return baseName + "-" + index;
  }

  private void testSortingNames(
      List<UUID> snapshotIds,
      String snapshotName,
      int offset,
      int limit,
      SqlSortDirection direction) {
    MetadataEnumeration<SnapshotSummary> summaryEnum =
        snapshotDao.retrieveSnapshots(
            offset,
            limit,
            EnumerateSortByParam.NAME,
            direction,
            null,
            null,
            datasetIds,
            snapshotIds);
    List<SnapshotSummary> summaryList = summaryEnum.getItems();
    int index = (direction.equals(SqlSortDirection.ASC)) ? offset : snapshotIds.size() - offset - 1;
    for (SnapshotSummary summary : summaryList) {
      assertThat("correct id", snapshotIds.get(index), equalTo(summary.getId()));
      assertThat("correct name", makeName(snapshotName, index), equalTo(summary.getName()));
      index += (direction.equals(SqlSortDirection.ASC)) ? 1 : -1;
    }
  }

  private void testSortingDescriptions(List<UUID> snapshotIds, SqlSortDirection direction) {
    MetadataEnumeration<SnapshotSummary> summaryEnum =
        snapshotDao.retrieveSnapshots(
            0, 6, EnumerateSortByParam.DESCRIPTION, direction, null, null, datasetIds, snapshotIds);
    List<SnapshotSummary> summaryList = summaryEnum.getItems();
    assertThat("the full list comes back", summaryList.size(), equalTo(6));
    String previous = summaryList.get(0).getDescription();
    for (int i = 1; i < summaryList.size(); i++) {
      String next = summaryList.get(i).getDescription();
      if (direction.equals(SqlSortDirection.ASC)) {
        assertThat("ascending order", previous, lessThan(next));
      } else {
        assertThat("descending order", previous, greaterThan(next));
      }
      previous = next;
    }
  }

  private void testOneEnumerateRange(
      List<UUID> snapshotIds, String snapshotName, int offset, int limit) {
    // We expect the snapshots to be returned in their created order
    MetadataEnumeration<SnapshotSummary> summaryEnum =
        snapshotDao.retrieveSnapshots(
            offset,
            limit,
            EnumerateSortByParam.CREATED_DATE,
            SqlSortDirection.ASC,
            null,
            null,
            datasetIds,
            snapshotIds);
    List<SnapshotSummary> summaryList = summaryEnum.getItems();
    int index = offset;
    for (SnapshotSummary summary : summaryList) {
      assertThat("correct snapshot id", snapshotIds.get(index), equalTo(summary.getId()));
      assertThat(
          "correct snapshot name", makeName(snapshotName, index), equalTo(summary.getName()));

      Map<CloudResource, StorageResource> storageMap =
          summary.getStorage().stream()
              .collect(Collectors.toMap(StorageResource::getCloudResource, Function.identity()));

      Snapshot fromDB = snapshotDao.retrieveSnapshot(snapshotIds.get(index));
      SnapshotSource source = fromDB.getFirstSnapshotSource();

      for (GoogleCloudResource resource : GoogleCloudResource.values()) {
        CloudRegion sourceRegion =
            source.getDataset().getDatasetSummary().getStorageResourceRegion(resource);
        CloudRegion snapshotRegion = storageMap.get(resource).getRegion();
        assertThat(
            "snapshot includes expected source dataset storage regions",
            snapshotRegion,
            equalTo(sourceRegion));
      }
      index++;
    }
  }

  @Test
  public void testPatchSnapshotConsentCode() throws Exception {
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());

    Snapshot snapshot =
        snapshotService
            .makeSnapshotFromSnapshotRequest(snapshotRequest)
            .projectResourceId(projectId)
            .id(snapshotId);
    String flightId = UUID.randomUUID().toString();
    snapshotDao.createAndLock(snapshot, flightId);
    snapshotDao.unlock(snapshotId, flightId);

    assertThat(
        "snapshot's consent code is null before patch",
        snapshotDao.retrieveSnapshot(snapshotId).getConsentCode(),
        equalTo(null));

    String consentCodeSet = "c01";
    SnapshotPatchRequestModel patchRequestSet =
        new SnapshotPatchRequestModel().consentCode(consentCodeSet);
    snapshotDao.patch(snapshotId, patchRequestSet);
    assertThat(
        "snapshot's consent code is set from patch",
        snapshotDao.retrieveSnapshot(snapshotId).getConsentCode(),
        equalTo(consentCodeSet));

    String consentCodeOverride = "c99";
    SnapshotPatchRequestModel patchRequestOverride =
        new SnapshotPatchRequestModel().consentCode(consentCodeOverride);
    snapshotDao.patch(snapshotId, patchRequestOverride);
    assertThat(
        "snapshot's consent code is overridden from patch",
        snapshotDao.retrieveSnapshot(snapshotId).getConsentCode(),
        equalTo(consentCodeOverride));

    snapshotDao.patch(snapshotId, new SnapshotPatchRequestModel());
    assertThat(
        "snapshot's consent code is unchanged when unspecified in patch request",
        snapshotDao.retrieveSnapshot(snapshotId).getConsentCode(),
        equalTo(consentCodeOverride));
  }
}
