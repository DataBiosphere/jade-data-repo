package bio.terra.service.snapshotbuilder;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.model.SnapshotBuilderSettings;
import java.util.List;
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

  public SnapshotBuilderGetConceptsResponse getConceptsFromParent(Integer parentId) {
    // TODO: Build real query - this should get the name and ID from the concept table, the count
    // from the occurrence table, and the existence of children from the concept_ancestor table.
    return new SnapshotBuilderGetConceptsResponse()
        .result(
            List.of(
                new SnapshotBuilderConcept()
                    .count(100)
                    .name("Stub concept")
                    .hasChildren(true)
                    .id(parentId + 1)));
  }
}
