package bio.terra.service.snapshotbuilder;

import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderSettings;
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
import bio.terra.service.snapshotbuilder.query.filtervariable.NotFilterVariable;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  private final SnapshotRequestDao snapshotRequestDao;
  private final SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;

  public SnapshotBuilderService(SnapshotRequestDao snapshotRequestDao, SnapshotBuilderSettingsDao snapshotBuilderSettingsDao) {
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
        .result(new SnapshotBuilderCountResponseResult().total(getRollupCountForCriteriaGroups(id, cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).collect(Collectors.toList()))));
  }

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  public int getRollupCountForCriteriaGroups(UUID datasetId, List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {
    SnapshotBuilderSettings snapshotBuilderSettings = snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId);

    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    FieldVariable personId = makePersonCountVariable();
    TablePointer conditionOccurrencePointer = TablePointer.fromTableName("condition_occurrence");
    TableVariable conditionOccurrenceVariable =
        TableVariable.forJoined(
            conditionOccurrencePointer,
            "person_id",
            new FieldVariable(new FieldPointer(tablePointer, "person_id"), tableVariable));

    TablePointer conditionAncestorPointer = TablePointer.fromTableName("concept_ancestor");
    TableVariable conditionAncestorVariable =
        TableVariable.forJoined(
            conditionAncestorPointer,
            "ancestor_concept_id",
            new FieldVariable(
                new FieldPointer(conditionOccurrencePointer, "condition_concept_id"),
                conditionOccurrenceVariable));

    Query query =
        new Query(
            List.of(
                makePersonCountVariable()),
            List.of(tableVariable, conditionOccurrenceVariable, conditionAncestorVariable),
            new BooleanAndOrFilterVariable(
                BooleanAndOrFilterVariable.LogicalOperator.OR,
                criteriaGroupsList.stream().map(this::generateFilterForCriteriaGroups).collect(Collectors.toList())

//                    new BooleanAndOrFilterVariable(
//                        BooleanAndOrFilterVariable.LogicalOperator.OR,
//                        List.of(
//                            new BinaryFilterVariable(
//                                new FieldVariable(
//                                    new FieldPointer(
//                                        conditionOccurrencePointer, "condition_concept_id"),
//                                    conditionOccurrenceVariable),
//                                BinaryFilterVariable.BinaryOperator.EQUALS,
//                                new Literal(316139)),
//                            new BinaryFilterVariable(
//                                new FieldVariable(
//                                    new FieldPointer(
//                                        conditionAncestorPointer, "ancestor_concept_id"),
//                                    conditionAncestorVariable),
//                                BinaryFilterVariable.BinaryOperator.EQUALS,
//                                new Literal(316139)),
//                            new BinaryFilterVariable(
//                                new FieldVariable(
//                                    new FieldPointer(
//                                        conditionOccurrencePointer, "condition_concept_id"),
//                                    conditionOccurrenceVariable),
//                                BinaryFilterVariable.BinaryOperator.EQUALS,
//                                new Literal(4311280)),
//                            new BinaryFilterVariable(
//                                new FieldVariable(
//                                    new FieldPointer(
//                                        conditionAncestorPointer, "ancestor_concept_id"),
//                                    conditionAncestorVariable),
//                                BinaryFilterVariable.BinaryOperator.EQUALS,
//                                new Literal(4311280)))),
//                    new BinaryFilterVariable(
//                        new FieldVariable(
//                            new FieldPointer(tablePointer, "year_of_birth"), tableVariable),
//                        BinaryFilterVariable.BinaryOperator.LESS_THAN,
//                        new Literal(1983))
            ));
    String sql = """
        SELECT DISTINCT p.person_id
        """;
    return 5;
  }

  private FilterVariable generateFilterForCriteriaGroups(List<SnapshotBuilderCriteriaGroup> criteriaGroups) {
    return new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.AND, criteriaGroups.stream().map(this::generateFilterForCriteriaGroup).collect(Collectors.toList()));
  }

  private FilterVariable generateFilterForCriteriaGroup(SnapshotBuilderCriteriaGroup criteriaGroup) {
    if (criteriaGroup.isMustMeet()) {
      return generateAndOrFilterForCriteriaGroup(criteriaGroup);
    } else {
      return new NotFilterVariable(generateAndOrFilterForCriteriaGroup(criteriaGroup));
    }
  }

  private FilterVariable generateAndOrFilterForCriteriaGroup(SnapshotBuilderCriteriaGroup criteriaGroup) {
    return new BooleanAndOrFilterVariable(criteriaGroup.isMeetAll() ? BooleanAndOrFilterVariable.LogicalOperator.AND : BooleanAndOrFilterVariable.LogicalOperator.OR, criteriaGroup.getCriteria().stream().map(this::generateFilterForCriteria).collect(Collectors.toList()));
  }

  private FilterVariable generateFilterForCriteria(SnapshotBuilderCriteria criteria) {
    switch(criteria.getKind()) {
      case LIST -> {
        TablePointer tablePointer = TablePointer.fromTableName("person");
        TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
        TablePointer conceptPointer = TablePointer.fromTableName("concept");
        TableVariable conceptPointerVariable =
            TableVariable.forJoined(
                conceptPointer,
                "concept_id",
                new FieldVariable(new FieldPointer(tablePointer, criteria.getName()), tableVariable));
        SnapshotBuilderProgramDataListCriteria listCriteria = (SnapshotBuilderProgramDataListCriteria) criteria;
        return new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.IN,
            new FieldVariable(
                new FieldPointer(conceptPointer, "concept_id"), conceptPointerVariable),
            listCriteria.getValues().stream().map(value -> new Literal(value.doubleValue())).toArray(Literal[]::new));
      }
      case RANGE -> {
        TablePointer tablePointer = TablePointer.fromTableName("person");
        TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
        SnapshotBuilderProgramDataRangeCriteria rangeCriteria = (SnapshotBuilderProgramDataRangeCriteria) criteria;
        return new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.AND,
            List.of(
                new BinaryFilterVariable(new FieldVariable(new FieldPointer(tablePointer, rangeCriteria.getName()), tableVariable), BinaryFilterVariable.BinaryOperator.GREATER_THAN_OR_EQUAL, new Literal(rangeCriteria.getLow().doubleValue())),
                new BinaryFilterVariable(new FieldVariable(new FieldPointer(tablePointer, rangeCriteria.getName()), tableVariable), BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL, new Literal(rangeCriteria.getHigh().doubleValue()))
            ));
      }
      case DOMAIN -> {
        TablePointer tablePointer = TablePointer.fromTableName("person");
        TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

      }
    }
  }

  private FieldVariable makePersonCountVariable() {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    return new FieldVariable(
        new FieldPointer(tablePointer, "person_id", "COUNT"),
        tableVariable,
        null,
        true);
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
