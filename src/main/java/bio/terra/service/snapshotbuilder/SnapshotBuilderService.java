package bio.terra.service.snapshotbuilder;

import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import java.util.List;
import java.util.UUID;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  private final SnapshotRequestDao snapshotRequestDao;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao, DatasetService datasetService, BigQueryDatasetPdao bigQueryDatasetPdao) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
  }

  public SnapshotAccessRequestResponse createSnapshotRequest(
      UUID id, SnapshotAccessRequest snapshotAccessRequest, String email) {
    return snapshotRequestDao.create(id, snapshotAccessRequest, email);
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(UUID datasetId, Integer conceptId) {
    // TODO: Build real query - this should get the name and ID from the concept table, the count
    // from the occurrence table, and the existence of children from the concept_ancestor table.
    Dataset dataset = datasetService.retrieve(datasetId);

    // TODO - replace manual query with query builder
    var bqTablePrefix =
        String.format(
            "%s.%s",
            dataset.getProjectResource().getGoogleProjectId(),
            BigQueryPdao.prefixName(dataset.getName()));

    var bigQuerySQL =
        String.format(
            """
        SELECT c.concept_name, c.concept_id FROM `%s.concept` AS c
        WHERE c.concept_id IN
        (SELECT c.descendant_concept_id FROM `%s.concept_ancestor` AS c
        WHERE c.ancestor_concept_id = 2);
        """,
            bqTablePrefix, bqTablePrefix);

    final List<SnapshotBuilderConcept> concepts =
        bigQueryDatasetPdao.runSnapshotBuilderQuery(
            bigQuerySQL, dataset, bigQueryDatasetPdao.aggregateSnapshotBuilderConceptResults());
    return new SnapshotBuilderGetConceptsResponse()
        .result(concepts);
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

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  private EnumerateSnapshotAccessRequest convertToEnumerateModel(
      List<SnapshotAccessRequestResponse> responses) {
    EnumerateSnapshotAccessRequest enumerateModel = new EnumerateSnapshotAccessRequest();
    for (SnapshotAccessRequestResponse response : responses) {
      enumerateModel.addItemsItem(
          new EnumerateSnapshotAccessRequestItem()
              .id(response.getId())
              .status(response.getStatus())
              .createdDate(response.getCreatedDate())
              .name(response.getSnapshotName())
              .researchPurpose(response.getSnapshotResearchPurpose())
              .createdBy(response.getCreatedBy()));
    }
    return enumerateModel;
  }
}
