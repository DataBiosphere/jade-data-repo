package bio.terra.service.snapshotbuilder;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  public SnapshotBuilderService() {}

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
