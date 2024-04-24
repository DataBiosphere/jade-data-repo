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

  public Dataset createMinimalDataset() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    BillingProfileModel billingProfile =
        profileDao.createBillingProfile(profileRequest, "testUser");

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    return createMinimalDataset(billingProfile.getId(), projectId);
  }

  public Snapshot createSnapshotFromDataset(Dataset dataset) throws IOException {
    SnapshotRequestModel snapshotRequest =
        jsonLoader.loadObject("snapshot-from-dataset-minimal.json", SnapshotRequestModel.class);

    String newName = snapshotRequest.getName() + UUID.randomUUID();
    snapshotRequest.name(newName).profileId(dataset.getDefaultProfileId());
    snapshotRequest.getContents().get(0).setDatasetName(dataset.getName());

    Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequest);
    snapshot.id(UUID.randomUUID());
    snapshot.projectResourceId(dataset.getProjectResourceId());

    String createFlightId = UUID.randomUUID().toString();

    snapshotDao.createAndLock(snapshot, createFlightId);
    snapshotDao.unlock(snapshot.getId(), createFlightId);
    return snapshotDao.retrieveSnapshot(snapshot.getId());
  }

  public Dataset createMinimalDataset(UUID billingProfileId, UUID projectResourceId)
      throws IOException {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
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

    return dataset;
  }
}
