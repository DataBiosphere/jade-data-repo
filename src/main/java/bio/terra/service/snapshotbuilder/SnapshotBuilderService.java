package bio.terra.service.snapshotbuilder;

import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.NotFilterVariable;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  private final SnapshotRequestDao snapshotRequestDao;
  private final SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao,
      SnapshotBuilderSettingsDao snapshotBuilderSettingsDao) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
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

  public SnapshotBuilderCountResponse getCountResponse(
      UUID id, List<SnapshotBuilderCohort> cohorts) {
    return new SnapshotBuilderCountResponse()
        .sql("")
        .result(
            new SnapshotBuilderCountResponseResult()
                .total(
                    getRollupCountForCriteriaGroups(
                        id,
                        cohorts.stream()
                            .map(SnapshotBuilderCohort::getCriteriaGroups)
                            .collect(Collectors.toList()))));
  }

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  public int getRollupCountForCriteriaGroups(
      UUID datasetId, List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {
    Query query = generateRollupCountsQueryForCriteriaGroupsList(criteriaGroupsList);
    query.renderSQL();
    return 5;
  }

  @NotNull
  private Query generateRollupCountsQueryForCriteriaGroupsList(
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    FieldVariable personId = makePersonCountVariable();
    return new Query(
        List.of(personId),
        List.of(tableVariable),
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.OR,
            criteriaGroupsList.stream()
                .map(this::generateFilterForCriteriaGroups)
                .collect(Collectors.toList())));
  }

  private FilterVariable generateFilterForCriteriaGroups(
      List<SnapshotBuilderCriteriaGroup> criteriaGroups) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        criteriaGroups.stream()
            .map(this::generateFilterForCriteriaGroup)
            .collect(Collectors.toList()));
  }

  private FilterVariable generateFilterForCriteriaGroup(
      SnapshotBuilderCriteriaGroup criteriaGroup) {
    if (criteriaGroup.isMustMeet()) {
      return generateAndOrFilterForCriteriaGroup(criteriaGroup);
    } else {
      return new NotFilterVariable(generateAndOrFilterForCriteriaGroup(criteriaGroup));
    }
  }

  private FilterVariable generateAndOrFilterForCriteriaGroup(
      SnapshotBuilderCriteriaGroup criteriaGroup) {
    return new BooleanAndOrFilterVariable(
        criteriaGroup.isMeetAll()
            ? BooleanAndOrFilterVariable.LogicalOperator.AND
            : BooleanAndOrFilterVariable.LogicalOperator.OR,
        criteriaGroup.getCriteria().stream()
            .map(CriteriaQueryUtils::generateFilterForCriteria)
            .collect(Collectors.toList()));
  }

  private FieldVariable makePersonCountVariable() {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    return new FieldVariable(
        new FieldPointer(tablePointer, "person_id", "COUNT"), tableVariable, null, true);
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
