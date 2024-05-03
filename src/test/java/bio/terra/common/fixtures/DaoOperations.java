package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class DaoOperations {
  private final JsonLoader jsonLoader;
  private final DatasetDao datasetDao;
  private final ProfileDao profileDao;
  private final GoogleResourceDao resourceDao;
  private final SnapshotDao snapshotDao;
  private final SnapshotService snapshotService;

  public static final String DATASET_MINIMAL = "dataset-minimal.json";
  public static final String SNAPSHOT_MINIMAL = "snapshot-from-dataset-minimal.json";

  public DaoOperations(
      JsonLoader jsonLoader,
      DatasetDao datasetDao,
      ProfileDao profileDao,
      GoogleResourceDao resourceDao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService) {
    this.jsonLoader = jsonLoader;
    this.datasetDao = datasetDao;
    this.profileDao = profileDao;
    this.resourceDao = resourceDao;
    this.snapshotDao = snapshotDao;
    this.snapshotService = snapshotService;
  }

  public Dataset createDataset(String path) throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    BillingProfileModel billingProfile =
        profileDao.createBillingProfile(profileRequest, "testUser");

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    return createDataset(billingProfile.getId(), projectId, path);
  }

  public Dataset createDataset(UUID billingProfileId, UUID projectResourceId, String path)
      throws IOException {
    DatasetRequestModel datasetRequest = jsonLoader.loadObject(path, DatasetRequestModel.class);
    String newName = datasetRequest.getName() + UUID.randomUUID();
    datasetRequest
        .name(newName)
        .defaultProfileId(billingProfileId)
        .cloudPlatform(CloudPlatform.GCP);
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectResourceId);
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);
    return datasetDao.retrieve(datasetId);
  }

  public SnapshotRequestModel createSnapshotRequestFromDataset(Dataset dataset, String snapshotPath)
      throws IOException {
    SnapshotRequestModel snapshotRequest =
        jsonLoader.loadObject(snapshotPath, SnapshotRequestModel.class);

    String newName = snapshotRequest.getName() + UUID.randomUUID();
    snapshotRequest.name(newName).profileId(dataset.getDefaultProfileId());
    snapshotRequest.getContents().get(0).setDatasetName(dataset.getName());

    return snapshotRequest;
  }

  public Snapshot createSnapshotFromSnapshotRequest(
      SnapshotRequestModel snapshotRequest, UUID projectResourceId) {
    Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequest);
    snapshot.id(UUID.randomUUID());
    snapshot.projectResourceId(projectResourceId);
    return snapshot;
  }

  public Snapshot ingestSnapshot(Snapshot snapshot) {
    String createFlightId = UUID.randomUUID().toString();
    snapshotDao.createAndLock(snapshot, createFlightId);
    snapshotDao.unlock(snapshot.getId(), createFlightId);
    return snapshotDao.retrieveSnapshot(snapshot.getId());
  }

  public Snapshot createAndIngestSnapshot(Dataset dataset, String snapshotPath) throws IOException {
    SnapshotRequestModel snapshotRequest = createSnapshotRequestFromDataset(dataset, snapshotPath);
    Snapshot snapshot =
        createSnapshotFromSnapshotRequest(snapshotRequest, dataset.getProjectResourceId());
    return ingestSnapshot(snapshot);
  }
}
