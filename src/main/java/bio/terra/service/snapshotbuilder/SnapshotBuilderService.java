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
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  private final SnapshotRequestDao snapshotRequestDao;

  public SnapshotBuilderService(SnapshotRequestDao snapshotRequestDao) {
    this.snapshotRequestDao = snapshotRequestDao;
  }

  public SnapshotAccessRequestResponse createSnapshotRequest(
      UUID id, SnapshotAccessRequest snapshotAccessRequest, String email) {
    return snapshotRequestDao.create(id, snapshotAccessRequest, email);
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

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  public SnapshotBuilderGetConceptsResponse searchConcepts(
      UUID datasetId, String domainId, String searchText) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept");
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldPointer nameFieldPointer = new FieldPointer(conceptTablePointer, "concept_name");
    FieldVariable nameFieldVariable = new FieldVariable(nameFieldPointer, conceptTableVariable);
    FieldPointer idFieldPointer = new FieldPointer(conceptTablePointer, "concept_id");
    FieldVariable idFieldVariable = new FieldVariable(idFieldPointer, conceptTableVariable);

    BinaryFilterVariable domainClause =
        new BinaryFilterVariable(
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "domain_id"), conceptTableVariable),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(domainId));
    FunctionFilterVariable searchConceptNameClause =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "concept_name"), conceptTableVariable),
            new Literal(searchText));
    FunctionFilterVariable searchConceptCodeClause =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "concept_code"), conceptTableVariable),
            new Literal(searchText));
    List<FilterVariable> searches = List.of(searchConceptNameClause, searchConceptCodeClause);
    BooleanAndOrFilterVariable searchClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.OR, searches);
    List<FilterVariable> allFilters = List.of(domainClause, searchClause);
    BooleanAndOrFilterVariable whereClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.AND, allFilters);

    Query query =
        new Query(
            List.of(nameFieldVariable, idFieldVariable),
            List.of(conceptTableVariable),
            whereClause);
    String sql = query.renderSQL();
    return new SnapshotBuilderGetConceptsResponse()
        .result(
            List.of(
                new SnapshotBuilderConcept()
                    .count(100)
                    .name("Stub concept")
                    .hasChildren(true)
                    .id(1)));
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
              .researchPurpose(response.getSnapshotResearchPurpose()));
    }
    return enumerateModel;
  }
}
