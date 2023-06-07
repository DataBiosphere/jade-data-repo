package bio.terra.service.snapshot;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
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
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.ResourceCreateTags;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.ras.RasDbgapPermissions;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.dataset.StorageResource;
import bio.terra.service.duos.DuosDao;
import bio.terra.service.filedata.DrsDao;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.snapshot.exception.SnapshotUpdateException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  @Autowired private DuosDao duosDao;

  @Autowired private DrsDao drsDao;

  private Dataset dataset;
  private UUID datasetId;
  private SnapshotRequestModel snapshotRequest;
  private List<UUID> snapshotIds;
  private List<UUID> datasetIds;
  private UUID profileId;
  private UUID projectId;
  private UUID duosFirecloudGroupId;
  private String duosId;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

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

    snapshotIds = new ArrayList<>();
    datasetIds = new ArrayList<>();

    duosId = UUID.randomUUID().toString();
    duosFirecloudGroupId =
        duosDao
            .insertAndRetrieveFirecloudGroup(
                new DuosFirecloudGroupModel()
                    .duosId(duosId)
                    .firecloudGroupName("firecloudGroupName")
                    .firecloudGroupEmail("firecloudGroupEmail"))
            .getId();
  }

  @After
  public void teardown() throws Exception {
    if (snapshotIds != null) {
      for (UUID id : snapshotIds) {
        snapshotDao.delete(id);
      }
    }
    datasetDao.delete(datasetId);
    resourceDao.deleteProject(projectId);
    profileDao.deleteBillingProfileById(profileId);
    duosDao.deleteFirecloudGroup(duosFirecloudGroupId);
  }

  private Snapshot createSnapshot(SnapshotRequestModel request) {
    UUID snapshotId = UUID.randomUUID();
    Snapshot snapshot =
        snapshotService
            .makeSnapshotFromSnapshotRequest(request)
            .projectResourceId(projectId)
            .id(snapshotId);

    String createFlightId = UUID.randomUUID().toString();

    return insertAndRetrieveSnapshot(snapshot, createFlightId);
  }

  private Snapshot insertAndRetrieveSnapshot(Snapshot snapshot, String flightId) {
    snapshotDao.createAndLock(snapshot, flightId);
    snapshotDao.unlock(snapshot.getId(), flightId);
    snapshotIds.add(snapshot.getId());
    return snapshotDao.retrieveSnapshot(snapshot.getId());
  }

  @Test
  public void testRetrieveSnapshotsForDataset() {
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());
    createSnapshot(snapshotRequest);
    List<SnapshotSummary> snapshots = snapshotDao.retrieveSnapshotsForDataset(datasetId);
    assertThat("there should exist one snapshot", snapshots, hasSize(1));
  }

  @Test
  public void happyInOutTest() {
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());

    Snapshot snapshot =
        snapshotService
            .makeSnapshotFromSnapshotRequest(snapshotRequest)
            .projectResourceId(projectId)
            .id(UUID.randomUUID());
    Snapshot fromDb = insertAndRetrieveSnapshot(snapshot, "happyInOutTest_flightId");
    assertThat("snapshot name set correctly", fromDb.getName(), equalTo(snapshot.getName()));

    assertThat(
        "snapshot description set correctly",
        fromDb.getDescription(),
        equalTo(snapshot.getDescription()));

    assertThat("correct number of tables created", fromDb.getTables(), hasSize(2));

    assertThat("correct number of sources created", fromDb.getSnapshotSources(), hasSize(1));

    assertThat(
        "snapshot creation information set correctly",
        fromDb.getCreationInformation(),
        equalTo(snapshot.getCreationInformation()));

    // verify source and map
    SnapshotSource source = fromDb.getFirstSnapshotSource();
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

    assertThat("correct number of map tables", source.getSnapshotMapTables(), hasSize(2));

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

    assertThat("correct number of map columns", mapTable.getSnapshotMapColumns(), hasSize(3));

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

    List<Relationship> relationships = fromDb.getRelationships();
    assertThat("a relationship comes back", relationships, hasSize(1));
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
        snapshotTable1.getColumns().stream().map(Column::getName).toList(),
        contains("thecolumn1", "thecolumn2", "thecolumn3"));

    assertThat(
        "Second table columns are in descending order of name",
        snapshotTable2.getColumns().stream().map(Column::getName).toList(),
        contains("anothercolumn3", "anothercolumn2", "anothercolumn1"));
  }

  @Test
  public void snapshotEnumerateTest() {
    String snapshotName = snapshotRequest.getName() + UUID.randomUUID();
    ResourceCreateTags tags = new ResourceCreateTags();
    tags.addAll(List.of("a tag", "A TAG"));

    for (int i = 0; i < 6; i++) {
      snapshotRequest
          .name(makeName(snapshotName, i))
          // set the description to a random string so we can verify the sorting is working
          // independently of the dataset name or created_date. add a suffix to filter on
          // for the even snapshots
          .description(UUID.randomUUID() + ((i % 2 == 0) ? "==foo==" : ""));
      if (i == 0) {
        // Only tag our first snapshot
        snapshotRequest.tags(tags);
      } else {
        snapshotRequest.tags(null);
      }
      createSnapshot(snapshotRequest);
    }
    MetadataEnumeration<SnapshotSummary> allSnapshots =
        snapshotDao.retrieveSnapshots(0, 6, null, null, null, null, datasetIds, snapshotIds, null);

    for (var snapshotSummary : allSnapshots.getItems()) {
      assertThat(
          "snapshot summary has the GCP cloud platform",
          snapshotSummary.getCloudPlatform(),
          equalTo(CloudPlatform.GCP));
      assertThat(
          "snapshot summary has a data project", snapshotSummary.getDataProject(), notNullValue());
    }

    testOneEnumerateRange(snapshotName, 0, 1000);
    testOneEnumerateRange(snapshotName, 1, 3);
    testOneEnumerateRange(snapshotName, 3, 5);
    testOneEnumerateRange(snapshotName, 4, 7);

    testSortingNames(snapshotName, 0, 10, SqlSortDirection.ASC);
    testSortingNames(snapshotName, 0, 3, SqlSortDirection.ASC);
    testSortingNames(snapshotName, 1, 3, SqlSortDirection.ASC);
    testSortingNames(snapshotName, 2, 5, SqlSortDirection.ASC);
    testSortingNames(snapshotName, 0, 10, SqlSortDirection.DESC);
    testSortingNames(snapshotName, 0, 3, SqlSortDirection.DESC);
    testSortingNames(snapshotName, 1, 3, SqlSortDirection.DESC);
    testSortingNames(snapshotName, 2, 5, SqlSortDirection.DESC);

    testSortingDescriptions(SqlSortDirection.DESC);
    testSortingDescriptions(SqlSortDirection.ASC);

    MetadataEnumeration<SnapshotSummary> filterDefaultRegionEnum =
        snapshotDao.retrieveSnapshots(
            0,
            6,
            null,
            null,
            null,
            GoogleRegion.DEFAULT_GOOGLE_REGION.toString(),
            datasetIds,
            snapshotIds,
            null);
    List<SnapshotSummary> filteredRegionSnapshots = filterDefaultRegionEnum.getItems();
    assertThat(
        "snapshot filter by default GCS region returns correct total",
        filteredRegionSnapshots,
        hasSize(snapshotIds.size()));
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
            snapshotIds,
            null);
    List<SnapshotSummary> filteredNameAndRegionSnapshots = filterNameAndRegionEnum.getItems();
    assertThat(
        "snapshot filter by name and region returns correct total",
        filteredNameAndRegionSnapshots,
        hasSize(1));
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

      verifySnapshotProject(snapshot);
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
            snapshotIds,
            null);
    List<SnapshotSummary> summaryList = summaryEnum.getItems();
    assertThat("filtered and retrieved 2 snapshots", summaryList, hasSize(2));
    assertThat("filtered total 3", summaryEnum.getFilteredTotal(), equalTo(3));
    assertThat("total 6", summaryEnum.getTotal(), equalTo(6));
    for (int i = 0; i < 2; i++) {
      assertThat("first 2 ids match", snapshotIds.get(i * 2), equalTo(summaryList.get(i).getId()));
    }

    MetadataEnumeration<SnapshotSummary> emptyEnum =
        snapshotDao.retrieveSnapshots(0, 6, null, null, "__", null, datasetIds, snapshotIds, null);
    assertThat("underscores don't act as wildcards", emptyEnum.getItems(), empty());

    MetadataEnumeration<SnapshotSummary> summaryEnum0 =
        snapshotDao.retrieveSnapshots(0, 10, null, null, null, null, datasetIds, snapshotIds, null);
    assertThat("no dataset uuid gives all snapshots", summaryEnum0.getTotal(), equalTo(6));

    // use the original dataset id and make sure you get all snapshots
    datasetIds = singletonList(datasetId);
    MetadataEnumeration<SnapshotSummary> summaryEnum1 =
        snapshotDao.retrieveSnapshots(0, 10, null, null, null, null, datasetIds, snapshotIds, null);
    assertThat(
        "expected dataset uuid gives expected snapshot", summaryEnum1.getTotal(), equalTo(6));

    // made a random dataset uuid and made sure that you get no snapshots
    List<UUID> datasetIdsBad = singletonList(UUID.randomUUID());
    MetadataEnumeration<SnapshotSummary> summaryEnum2 =
        snapshotDao.retrieveSnapshots(
            0, 10, null, null, null, null, datasetIdsBad, snapshotIds, null);
    assertThat("dummy dataset uuid gives no snapshots", summaryEnum2.getTotal(), equalTo(0));

    // Test filtering by tags

    // Filtering on all tags for a snapshot returns the snapshot
    MetadataEnumeration<SnapshotSummary> filteredAllTagsMatchEnum =
        snapshotDao.retrieveSnapshots(0, 6, null, null, null, null, datasetIds, snapshotIds, tags);
    List<SnapshotSummary> filteredAllTagsMatchSnapshots = filteredAllTagsMatchEnum.getItems();
    assertThat(
        "snapshot filter by tags returns correct filtered total",
        filteredAllTagsMatchSnapshots,
        hasSize(1));
    assertThat(
        "snapshot filter by tags returns correct snapshot",
        filteredAllTagsMatchSnapshots.get(0).getId(),
        equalTo(snapshotIds.get(0)));
    assertThat(
        "snapshot filter by tags returns correct total",
        filteredAllTagsMatchEnum.getTotal(),
        equalTo(6));

    // Filtering on a strict subset of tags for a dataset returns the dataset
    tags.forEach(
        tag -> {
          MetadataEnumeration<SnapshotSummary> filteredSubsetTagsMatchEnum =
              snapshotDao.retrieveSnapshots(
                  0, 6, null, null, null, null, datasetIds, snapshotIds, List.of(tag));
          List<SnapshotSummary> filteredSubsetTagsMatchSnapshots =
              filteredSubsetTagsMatchEnum.getItems();
          assertThat(
              "snapshot filter by tags returns correct filtered total",
              filteredSubsetTagsMatchSnapshots,
              hasSize(1));
          assertThat(
              "snapshot filter by tags returns correct snapshot",
              filteredSubsetTagsMatchSnapshots.get(0).getId(),
              equalTo(snapshotIds.get(0)));
          assertThat(
              "snapshot filter by tags returns correct total",
              filteredSubsetTagsMatchEnum.getTotal(),
              equalTo(6));
        });

    // If even one specified tag is not found on the snapshot, it's not returned, even if other
    // matching tags are included
    tags.add(UUID.randomUUID().toString());
    MetadataEnumeration<SnapshotSummary> incompleteTagMatchEnum =
        snapshotDao.retrieveSnapshots(0, 6, null, null, null, null, datasetIds, snapshotIds, tags);
    assertThat(
        "snapshot filter by tags excludes snapshot which do not match filter completely",
        incompleteTagMatchEnum.getItems(),
        empty());
    assertThat(
        "snapshot filter by tags returns correct total",
        incompleteTagMatchEnum.getTotal(),
        equalTo(6));
  }

  private void verifySnapshotProject(Snapshot snapshot) {
    SnapshotProject snapshotProject = snapshotDao.retrieveSnapshotProject(snapshot.getId());
    assertThat("snapshot project id matches", snapshotProject.getId(), equalTo(snapshot.getId()));
    assertThat(
        "snapshot project name matches", snapshotProject.getName(), equalTo(snapshot.getName()));
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
        snapshotProject.getSourceDatasetProjects(),
        hasSize(1));
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

  private String makeName(String baseName, int index) {
    return baseName + "-" + index;
  }

  private void testSortingNames(
      String snapshotName, int offset, int limit, SqlSortDirection direction) {
    MetadataEnumeration<SnapshotSummary> summaryEnum =
        snapshotDao.retrieveSnapshots(
            offset,
            limit,
            EnumerateSortByParam.NAME,
            direction,
            null,
            null,
            datasetIds,
            snapshotIds,
            null);
    List<SnapshotSummary> summaryList = summaryEnum.getItems();
    int index = (direction.equals(SqlSortDirection.ASC)) ? offset : snapshotIds.size() - offset - 1;
    for (SnapshotSummary summary : summaryList) {
      assertThat("correct id", snapshotIds.get(index), equalTo(summary.getId()));
      assertThat("correct name", makeName(snapshotName, index), equalTo(summary.getName()));
      index += (direction.equals(SqlSortDirection.ASC)) ? 1 : -1;
    }
  }

  private void testSortingDescriptions(SqlSortDirection direction) {
    MetadataEnumeration<SnapshotSummary> summaryEnum =
        snapshotDao.retrieveSnapshots(
            0,
            6,
            EnumerateSortByParam.DESCRIPTION,
            direction,
            null,
            null,
            datasetIds,
            snapshotIds,
            null);
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

  private void testOneEnumerateRange(String snapshotName, int offset, int limit) {
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
            snapshotIds,
            null);
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
  public void testPatchSnapshotConsentCodeAndDescription() {
    String defaultSnapshotDescription = "A meaningful description of a snapshot.";
    snapshotRequest
        .name(snapshotRequest.getName() + UUID.randomUUID())
        .description(defaultSnapshotDescription);

    Snapshot created = createSnapshot(snapshotRequest);
    UUID snapshotId = created.getId();

    assertNull("snapshot's consent code is null before patch", created.getConsentCode());
    assertThat(
        "snapshot's default description is correct before any patch",
        created.getDescription(),
        equalTo(defaultSnapshotDescription));

    String consentCodeSet = "c01";
    SnapshotPatchRequestModel patchRequestSet =
        new SnapshotPatchRequestModel().consentCode(consentCodeSet);
    snapshotDao.patch(snapshotId, patchRequestSet, TEST_USER);
    Snapshot snapshotConsentCodeSet = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "snapshot's consent code is set from patch",
        snapshotConsentCodeSet.getConsentCode(),
        equalTo(consentCodeSet));
    assertThat(
        "snapshot's description remains unmodified when updating consent code.",
        snapshotConsentCodeSet.getDescription(),
        equalTo(defaultSnapshotDescription));

    String consentCodeOverride = "c99";
    SnapshotPatchRequestModel patchRequestOverride =
        new SnapshotPatchRequestModel().consentCode(consentCodeOverride);
    snapshotDao.patch(snapshotId, patchRequestOverride, TEST_USER);
    Snapshot snapshotConsentCodeOverride = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "snapshot's consent code is overridden from patch",
        snapshotConsentCodeOverride.getConsentCode(),
        equalTo(consentCodeOverride));

    snapshotDao.patch(snapshotId, new SnapshotPatchRequestModel(), TEST_USER);
    Snapshot snapshotEmptyPatch = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "snapshot's consent code is unchanged when unspecified in patch request",
        snapshotEmptyPatch.getConsentCode(),
        equalTo(consentCodeOverride));
    assertThat(
        "snapshot's description is unchanged when unspecified in patch request",
        snapshotEmptyPatch.getDescription(),
        equalTo(defaultSnapshotDescription));

    SnapshotPatchRequestModel patchRequestBlank = new SnapshotPatchRequestModel().consentCode("");
    snapshotDao.patch(snapshotId, patchRequestBlank, TEST_USER);
    Snapshot snapshotConsentCodeBlank = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "snapshot's consent code is set to empty string from patch",
        snapshotConsentCodeBlank.getConsentCode(),
        equalTo(""));

    SnapshotPatchRequestModel patchDescription =
        new SnapshotPatchRequestModel().description("A new description");
    snapshotDao.patch(snapshotId, patchDescription, TEST_USER);
    Snapshot snapshotPatchDescription = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "snapshot's description is updated",
        snapshotPatchDescription.getDescription(),
        equalTo("A new description"));
    assertThat(
        "snapshot's consent code is still set to empty string from last consent code patch",
        snapshotPatchDescription.getConsentCode(),
        equalTo(""));

    SnapshotPatchRequestModel patchDescAndCode =
        new SnapshotPatchRequestModel().consentCode("c99").description("Another new description");
    snapshotDao.patch(snapshotId, patchDescAndCode, TEST_USER);
    Snapshot snapshotPatchDescAndCode = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "snapshot's description is updated",
        snapshotPatchDescAndCode.getDescription(),
        equalTo("Another new description"));
    assertThat(
        "snapshot's consent code is updated",
        snapshotPatchDescAndCode.getConsentCode(),
        equalTo("c99"));

    SnapshotPatchRequestModel patchZeroLenStrDesc = new SnapshotPatchRequestModel().description("");
    snapshotDao.patch(snapshotId, patchZeroLenStrDesc, TEST_USER);
    Snapshot snapshotDescriptionBlank = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "snapshot's description is updated to empty string",
        snapshotDescriptionBlank.getDescription(),
        equalTo(""));
  }

  @Test
  public void createSnapshotWithProperties() {
    String properties =
        "{\"projectName\":\"project\", " + "\"authors\": [\"harry\", \"ron\", \"hermionie\"]}";
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID()).properties(properties);
    Snapshot snapshot =
        snapshotService
            .makeSnapshotFromSnapshotRequest(snapshotRequest)
            .projectResourceId(projectId)
            .id(UUID.randomUUID());
    String flightId = UUID.randomUUID().toString();
    Snapshot fromDB = insertAndRetrieveSnapshot(snapshot, flightId);
    assertThat(
        "snapshot properties set correctly",
        fromDB.getProperties(),
        equalTo(snapshot.getProperties()));
  }

  @Test
  public void patchSnapshotProperties() {
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());
    Snapshot fromDb = createSnapshot(snapshotRequest);
    UUID snapshotId = fromDb.getId();
    assertNull("snapshot properties is null before patch", fromDb.getProperties());

    String updatedProperties = "{\"projectName\":\"updatedProject\"}";
    SnapshotPatchRequestModel patchRequestSet =
        new SnapshotPatchRequestModel().properties(updatedProperties);
    snapshotDao.patch(snapshotId, patchRequestSet, TEST_USER);
    assertThat(
        "snapshot properties is set from patch",
        snapshotDao.retrieveSnapshot(snapshotId).getProperties(),
        equalTo(updatedProperties));

    SnapshotPatchRequestModel patchRequestNull = new SnapshotPatchRequestModel().consentCode("c01");
    snapshotDao.patch(datasetId, patchRequestNull, TEST_USER);
    assertThat(
        "snapshot properties is unchanged when not in request",
        snapshotDao.retrieveSnapshot(snapshotId).getProperties(),
        equalTo(updatedProperties));

    SnapshotPatchRequestModel patchRequestExplicitNull =
        new SnapshotPatchRequestModel().properties(null);
    snapshotDao.patch(snapshotId, patchRequestExplicitNull, TEST_USER);
    assertThat(
        "snapshot properties is unchanged if set to null",
        snapshotDao.retrieveSnapshot(snapshotId).getProperties(),
        equalTo(updatedProperties));

    Object unsetDatasetProperties = jsonLoader.loadJson("{}", new TypeReference<>() {});
    SnapshotPatchRequestModel patchRequestUnset =
        new SnapshotPatchRequestModel().properties(unsetDatasetProperties);
    snapshotDao.patch(snapshotId, patchRequestUnset, TEST_USER);
    assertThat(
        "snapshot properties is set to empty",
        snapshotDao.retrieveSnapshot(snapshotId).getProperties(),
        equalTo(unsetDatasetProperties));
  }

  @Test
  public void getAccessibleSnapshots() {
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());
    UUID snapshotId = createSnapshot(snapshotRequest).getId();

    String consentCode = "c01";
    String phsId = "phs123456";

    // Partially populated RasDbGapPermissions should not yield matching snapshots:
    // We only return snapshots whose permission criteria are fully populated.
    // This should never occur if ECM only returns valid passports and visas as indicated by their
    // Swagger documentation. Testing it anyway to verify our own behavior.
    List<RasDbgapPermissions> permissions =
        List.of(
            new RasDbgapPermissions(consentCode, phsId),
            new RasDbgapPermissions(null, phsId),
            new RasDbgapPermissions(consentCode, null));

    assertThat(
        "No snapshots returned when no permissions",
        snapshotDao.getAccessibleSnapshots(List.of()),
        empty());

    assertThat(
        "Snapshot with all permission elements missing is inaccessible",
        snapshotDao.getAccessibleSnapshots(permissions),
        empty());

    SnapshotPatchRequestModel patchRequestConsentCode =
        new SnapshotPatchRequestModel().consentCode(consentCode);
    snapshotDao.patch(snapshotId, patchRequestConsentCode, TEST_USER);

    assertThat(
        "Snapshot with partial permission match is inaccessible",
        snapshotDao.getAccessibleSnapshots(permissions),
        empty());

    DatasetPatchRequestModel patchRequestPhsId = new DatasetPatchRequestModel().phsId(phsId);
    datasetDao.patch(datasetId, patchRequestPhsId, TEST_USER);

    assertThat(
        "Snapshot with full permission match is accessible",
        snapshotDao.getAccessibleSnapshots(permissions),
        contains(snapshotId));
  }

  @Test
  public void updateDuosFirecloudGroupId() {
    assertThrows(
        "Exception is thrown when updating nonexistent snapshot",
        SnapshotUpdateException.class,
        () -> snapshotDao.updateDuosFirecloudGroupId(UUID.randomUUID(), duosFirecloudGroupId));

    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());
    Snapshot beforeUpdate = createSnapshot(snapshotRequest);
    UUID snapshotId = beforeUpdate.getId();

    assertNull(
        "snapshot's DUOS Firecloud group ID is null before update",
        beforeUpdate.getDuosFirecloudGroupId());
    assertNull(
        "snapshot's DUOS Firecloud group is null before update",
        beforeUpdate.getDuosFirecloudGroup());

    snapshotDao.updateDuosFirecloudGroupId(snapshotId, duosFirecloudGroupId);

    Snapshot afterUpdate = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(afterUpdate.getDuosFirecloudGroupId(), equalTo(duosFirecloudGroupId));
    assertThat(
        "Linked DUOS Firecloud group is obtained",
        afterUpdate.getDuosFirecloudGroup().getDuosId(),
        equalTo(duosId));

    snapshotDao.updateDuosFirecloudGroupId(snapshotId, null);

    Snapshot afterUnset = snapshotDao.retrieveSnapshot(snapshotId);
    assertNull(
        "snapshot's DUOS Firecloud group ID is null after unset",
        afterUnset.getDuosFirecloudGroupId());
    assertNull(
        "snapshot's DUOS Firecloud group is null after unset", afterUnset.getDuosFirecloudGroup());
  }

  @Test
  public void recordDrsIds() {
    // This test runs through a couple of scenarios.  It's in one method to avoid the setup overhead
    // Initialize test
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());
    Snapshot snapshot1 = createSnapshot(snapshotRequest);
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID());
    Snapshot snapshot2 = createSnapshot(snapshotRequest);

    List<DrsId> drsIds =
        IntStream.range(0, 1000)
            .boxed()
            .map(i -> DrsIdService.fromUri("drs://home/v2_" + i))
            .toList();

    // Load drs ids (attempt loading duplicates)
    assertThat(
        "drsIds were inserted into snp1",
        drsDao.recordDrsIdToSnapshot(snapshot1.getId(), drsIds),
        equalTo(1000L));
    assertThat(
        "drsIds were ignored on reinsert into snp1",
        drsDao.recordDrsIdToSnapshot(snapshot1.getId(), drsIds),
        equalTo(0L));

    assertThrows(
        "can't insert id for an invalid snapshot",
        Exception.class,
        () -> drsDao.recordDrsIdToSnapshot(UUID.randomUUID(), drsIds));

    // A subset of drs ids are in snapshot 2
    assertThat(
        "drsIds were inserted into snp2",
        drsDao.recordDrsIdToSnapshot(snapshot2.getId(), drsIds.subList(0, 100)),
        equalTo(100L));
    assertThat(
        "new drsIds were inserted into snp2",
        drsDao.recordDrsIdToSnapshot(snapshot2.getId(), drsIds.subList(50, 150)),
        equalTo(50L));

    // Drs IDs are linked to correct snapshots
    assertThat(
        "lower numbered drs id is in snapshot 1 and snapshot 2",
        drsDao.retrieveReferencedSnapshotIds(drsIds.get(0)),
        containsInAnyOrder(snapshot1.getId(), snapshot2.getId()));

    // Drs IDs are linked to correct snapshots
    assertThat(
        "higher numbered drs id is in snapshot 1 only",
        drsDao.retrieveReferencedSnapshotIds(drsIds.get(500)),
        containsInAnyOrder(snapshot1.getId()));

    // Deleting snapshot 1 removes entries
    assertThat(
        "all 1000 entries removed when deleting the first snapshot",
        drsDao.deleteDrsIdToSnapshotsBySnapshot(snapshot1.getId()),
        equalTo(1000L));
    assertThat(
        "lower numbered drs id is now only in snapshot 2",
        drsDao.retrieveReferencedSnapshotIds(drsIds.get(0)),
        containsInAnyOrder(snapshot2.getId()));

    // Deleting snapshot 2 removes entries
    assertThat(
        "all 150 entries removed when deleting the second snapshot",
        drsDao.deleteDrsIdToSnapshotsBySnapshot(snapshot2.getId()),
        equalTo(150L));
  }

  @Test
  public void getSnapshotIds() {
    assertThat(
        "No snapshots in DB yield an empty UUID list", snapshotDao.getSnapshotIds(), empty());

    String snapshotName = snapshotRequest.getName() + UUID.randomUUID();
    String flightId = "getSnapshotIds_flightId";
    for (int i = 0; i < 3; i++) {
      snapshotRequest.name(makeName(snapshotName, i));
      UUID snapshotId = createSnapshot(snapshotRequest).getId();
      snapshotDao.lock(snapshotId, flightId);
    }

    assertThat(
        "Locked snapshot UUIDs are returned",
        snapshotDao.getSnapshotIds(),
        containsInAnyOrder(snapshotIds.toArray()));

    snapshotIds.forEach(id -> snapshotDao.unlock(id, flightId));
    assertThat(
        "Unlocked snapshot UUIDs are returned",
        snapshotDao.getSnapshotIds(),
        containsInAnyOrder(snapshotIds.toArray()));
  }

  @Test
  public void createDatasetWithTags() {
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID()).tags(null);
    Snapshot snapshotNullTags = createSnapshot(snapshotRequest);
    verifyTags("null snapshot tags are returned as empty list", snapshotNullTags, List.of());

    snapshotRequest
        .name(snapshotRequest.getName() + UUID.randomUUID())
        .tags(new ResourceCreateTags());
    Snapshot snapshotEmptyTags = createSnapshot(snapshotRequest);
    verifyTags("empty snapshot tags are returned as empty list", snapshotEmptyTags, List.of());

    ResourceCreateTags tags = new ResourceCreateTags();
    tags.add(null);
    tags.addAll(List.of("a tag", "A TAG", "duplicate", "duplicate"));
    List<String> expectedTags = List.of("a tag", "A TAG", "duplicate");
    snapshotRequest.name(snapshotRequest.getName() + UUID.randomUUID()).tags(tags);
    Snapshot snapshotWithTags = createSnapshot(snapshotRequest);
    verifyTags("distinct non-null tags are returned", snapshotWithTags, expectedTags);
  }

  private void verifyTags(String reason, Snapshot snapshot, List<String> expectedTags) {
    assertThat(
        reason + " when retrieving snapshot",
        snapshot.getTags(),
        containsInAnyOrder(expectedTags.toArray()));

    assertThat(
        reason + " when retrieving snapshot summary",
        snapshotDao.retrieveSummaryById(snapshot.getId()).getTags(),
        containsInAnyOrder(expectedTags.toArray()));
  }

  @Test
  public void testRetrieveLockedSnapshot() {
    Snapshot snapshot = createSnapshot(snapshotRequest);
    UUID snapshotId = snapshot.getId();
    String flightId = "flightId";

    // After locking the snapshot, we should still be able to retrieve it
    snapshotDao.lock(snapshotId, flightId);

    Snapshot lockedSnapshot = snapshotDao.retrieveSnapshot(snapshotId);
    assertThat(
        "Locked snapshot can be retrieved", lockedSnapshot.getLockingJobId(), equalTo(flightId));

    SnapshotSummary lockedSnapshotSummary = snapshotDao.retrieveSummaryById(snapshotId);
    assertThat(
        "Locked snapshot summary can be retrieved",
        lockedSnapshotSummary.getLockingJobId(),
        equalTo(flightId));

    MetadataEnumeration<SnapshotSummary> snapshotEnumeration =
        snapshotDao.retrieveSnapshots(0, 1, null, null, null, null, datasetIds, snapshotIds, null);
    assertThat(snapshotEnumeration.getTotal(), equalTo(1));
    SnapshotSummary lockedSnapshotSummaryEnumerationItem = snapshotEnumeration.getItems().get(0);
    assertThat(
        "Locked snapshot summary can be enumerated",
        lockedSnapshotSummaryEnumerationItem.getLockingJobId(),
        equalTo(flightId));

    // Locked snapshot's project can be retrieved
    verifySnapshotProject(snapshot);
  }
}
