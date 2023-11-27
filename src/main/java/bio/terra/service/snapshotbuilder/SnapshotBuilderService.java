package bio.terra.service.snapshotbuilder;

import bio.terra.model.SnapshotBuilderAccessRequest;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
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

  public SnapshotBuilderAccessRequest createSnapshotRequest(
      // TODO: in DC-782 add given request to the database
      UUID id, SnapshotBuilderAccessRequest snapshotAccessRequest) {
    return snapshotAccessRequest;
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(UUID datasetId, Integer conceptId) {
    // TODO: Build real query - this should get the name and ID from the concept table, the count
    // from the occurrence table, and the existence of children from the concept_ancestor table.
    return new SnapshotBuilderGetConceptsResponse()
        .result(
            List.of(
                new SnapshotBuilderConcept()
                    .count(100)
                    .name("Stub concept")
                    .hasChildren(true)
                    .id(conceptId + 1)));
  }

  private int getRollupCount(UUID datasetId, List<SnapshotBuilderCohort> cohorts) {
    return 100;
  }

  public SnapshotBuilderCountResponse getCountResponse(
      UUID id, List<SnapshotBuilderCohort> cohorts) {
    return new SnapshotBuilderCountResponse()
        .sql("")
        .result(new SnapshotBuilderCountResponseResult().total(getRollupCount(id, cohorts)));
  }
}
