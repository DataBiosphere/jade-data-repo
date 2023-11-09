package bio.terra.service.snapshotbuilder;

import bio.terra.model.JobModel;
import bio.terra.model.SnapshotAccessRequest;
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


  public JobModel requestSnapshot(
      // TODO: add given request to the database, return real JobModel with the id of the new entry.
      UUID id, SnapshotAccessRequest snapshotAccessRequest) {
    return new JobModel()
        .id("id")
        .description("Stub Method")
        .jobStatus(JobModel.JobStatusEnum.SUCCEEDED)
        .statusCode(200)
        .completed("completed")
        .submitted("submitted")
        .className("SnapshotAccessRequest");

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
}
