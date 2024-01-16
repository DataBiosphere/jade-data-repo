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
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
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
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;
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
            .map(this::generateFilterForCriteria)
            .collect(Collectors.toList()));
  }

  private FilterVariable generateFilterForCriteria(SnapshotBuilderCriteria criteria) {
    switch (criteria.getKind()) {
      case LIST -> {
        SnapshotBuilderProgramDataListCriteria listCriteria =
            (SnapshotBuilderProgramDataListCriteria) criteria;
        return generateFilterForListCriteria(listCriteria);
      }
      case RANGE -> {
        SnapshotBuilderProgramDataRangeCriteria rangeCriteria =
            (SnapshotBuilderProgramDataRangeCriteria) criteria;
        return generateFilterForRangeCriteria(rangeCriteria);
      }
      case DOMAIN -> {
        SnapshotBuilderDomainCriteria domainCriteria = (SnapshotBuilderDomainCriteria) criteria;
        return generateFilterForDomainCriteria(domainCriteria);
      }
      default -> throw new BadRequestException(
          "Expected one of the range, list, or domain criteria");
    }
  }

  @NotNull
  private static BooleanAndOrFilterVariable generateFilterForRangeCriteria(
      SnapshotBuilderProgramDataRangeCriteria rangeCriteria) {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        List.of(
            new BinaryFilterVariable(
                new FieldVariable(
                    new FieldPointer(tablePointer, rangeCriteria.getName()), tableVariable),
                BinaryFilterVariable.BinaryOperator.GREATER_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getLow().doubleValue())),
            new BinaryFilterVariable(
                new FieldVariable(
                    new FieldPointer(tablePointer, rangeCriteria.getName()), tableVariable),
                BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getHigh().doubleValue()))));
  }

  @NotNull
  private static FunctionFilterVariable generateFilterForListCriteria(
      SnapshotBuilderProgramDataListCriteria listCriteria) {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    TablePointer conceptPointer = TablePointer.fromTableName("concept");
    TableVariable conceptPointerVariable =
        TableVariable.forJoined(
            conceptPointer,
            "concept_id",
            new FieldVariable(
                new FieldPointer(tablePointer, listCriteria.getName()), tableVariable));
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.IN,
        new FieldVariable(new FieldPointer(conceptPointer, "concept_id"), conceptPointerVariable),
        listCriteria.getValues().stream()
            .map(value -> new Literal(value.doubleValue()))
            .toArray(Literal[]::new));
  }

  private record OccurrenceTable(String tableName, String idColumnName) {}

  private OccurrenceTable getOccurrenceTableFromDomainCriteria(
      SnapshotBuilderDomainCriteria domainCriteria) {
    // TODO: Load this information from snapshot builder settings, rather than hardcode
    return switch (domainCriteria.getDomainName()) {
      case "Condition" -> new OccurrenceTable("condition_occurrence", "condition_concept_id");
      case "Measurement" -> new OccurrenceTable("measurement", "measurement_concept_id");
      case "Visit" -> new OccurrenceTable("visit_occurrence", "visit_concept_id");
      case "Procedure" -> new OccurrenceTable("procedure_occurrence", "procedure_concept_id");
      case "Observation" -> new OccurrenceTable("observation", "observation_concept_id");
      case "Device" -> new OccurrenceTable("device_exposure", "device_concept_id");
      case "Drug" -> new OccurrenceTable("drug_exposure", "drug_concept_id");
      default -> throw new BadRequestException(
          String.format("Domain %s is not found in dataset", domainCriteria.getDomainName()));
    };
  }

  @NotNull
  private SubQueryFilterVariable generateFilterForDomainCriteria(
      SnapshotBuilderDomainCriteria domainCriteria) {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    OccurrenceTable occurrenceTable = getOccurrenceTableFromDomainCriteria(domainCriteria);

    TablePointer occurrencePointer = TablePointer.fromTableName(occurrenceTable.tableName());
    TableVariable occurrenceVariable = TableVariable.forPrimary(occurrencePointer);

    TablePointer ancestorPointer = TablePointer.fromTableName("concept_ancestor");
    TableVariable ancestorVariable =
        TableVariable.forJoined(
            ancestorPointer,
            "ancestor_concept_id",
            new FieldVariable(
                new FieldPointer(occurrencePointer, occurrenceTable.idColumnName()),
                occurrenceVariable));

    return new SubQueryFilterVariable(
        new FieldVariable(new FieldPointer(tablePointer, "person_id"), tableVariable),
        SubQueryFilterVariable.Operator.IN,
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(occurrencePointer, "person_id"), occurrenceVariable)),
            List.of(occurrenceVariable, ancestorVariable),
            new BooleanAndOrFilterVariable(
                BooleanAndOrFilterVariable.LogicalOperator.OR,
                List.of(
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(occurrencePointer, occurrenceTable.idColumnName()),
                            occurrenceVariable),
                        BinaryFilterVariable.BinaryOperator.EQUALS,
                        new Literal(domainCriteria.getId().doubleValue())),
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(ancestorPointer, "ancestor_concept_id"),
                            ancestorVariable),
                        BinaryFilterVariable.BinaryOperator.EQUALS,
                        new Literal(domainCriteria.getId().doubleValue()))))));
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
