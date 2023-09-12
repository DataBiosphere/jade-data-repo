package bio.terra.service.snapshotbuilder;

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
}
