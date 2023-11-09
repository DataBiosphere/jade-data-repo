package bio.terra.service.snapshotbuilder;

import bio.terra.model.JobModel;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotBuilderSettings;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {
  private final SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;

  public SnapshotBuilderService(SnapshotBuilderSettingsDao snapshotBuilderSettingsDao) {
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
  }

  public SnapshotBuilderSettings getSnapshotBuilderSettings(UUID datasetId) {
    return snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId);
  }

  public SnapshotBuilderSettings updateSnapshotBuilderSettings(
      UUID id, SnapshotBuilderSettings settings) {
    return snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(id, settings);
  }

  public JobModel requestSnapshot(
      // TODO: complete stubbed method by adding given request to the database and returning the id
      // of the new entry.
      UUID id, SnapshotAccessRequest snapshotAccessRequest) {
    return new JobModel()
        .id("id")
        .description("Stub Method")
        .jobStatus(JobModel.JobStatusEnum.SUCCEEDED)
        .statusCode(200)
        .completed("completed")
        .submitted("submitted")
        .className("SnapshotAccessRequest");
  }
}
