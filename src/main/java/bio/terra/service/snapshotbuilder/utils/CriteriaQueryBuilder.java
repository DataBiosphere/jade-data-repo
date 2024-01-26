package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.NotFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.util.VisibleForTesting;

public class CriteriaQueryBuilder {
  private final String rootTableName;
  private final TableNameGenerator tableNameGenerator;
  final Map<String, TableVariable> tables = new HashMap<>();

  private static final Map<String, OccurrenceTable> DOMAIN_TO_OCCURRENCE_TABLE =
      Map.of(
          "Condition", new OccurrenceTable("condition_occurrence", "condition_concept_id"),
          "Measurement", new OccurrenceTable("measurement", "measurement_concept_id"),
          "Visit", new OccurrenceTable("visit_occurrence", "visit_concept_id"),
          "Procedure", new OccurrenceTable("procedure_occurrence", "procedure_concept_id"),
          "Observation", new OccurrenceTable("observation", "observation_concept_id"),
          "Device", new OccurrenceTable("device_exposure", "device_concept_id"),
          "Drug", new OccurrenceTable("drug_exposure", "drug_concept_id"));

  private static OccurrenceTable getOccurrenceTableFromDomain(String domain) {
    OccurrenceTable occurrenceTable = DOMAIN_TO_OCCURRENCE_TABLE.get(domain);
    if (occurrenceTable == null) {
      throw new BadRequestException(String.format("Domain %s is not found in dataset", domain));
    }
    return occurrenceTable;
  }

  CriteriaQueryBuilder(String rootTableName, TableNameGenerator tableNameGenerator) {
    this.rootTableName = rootTableName;
    this.tableNameGenerator = tableNameGenerator;
    TablePointer tablePointer = TablePointer.fromTableName(rootTableName, tableNameGenerator);
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    tables.put(rootTableName, tableVariable);
  }

  private TablePointer getRootTablePointer() {
    return getRootTableVariable().getTablePointer();
  }

  private TableVariable getRootTableVariable() {
    return tables.get(rootTableName);
  }

  @VisibleForTesting
  public BooleanAndOrFilterVariable generateFilterForRangeCriteria(
      SnapshotBuilderProgramDataRangeCriteria rangeCriteria) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        List.of(
            new BinaryFilterVariable(
                new FieldVariable(
                    new FieldPointer(getRootTablePointer(), rangeCriteria.getName()),
                    getRootTableVariable()),
                BinaryFilterVariable.BinaryOperator.GREATER_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getLow().intValue())),
            new BinaryFilterVariable(
                new FieldVariable(
                    new FieldPointer(getRootTablePointer(), rangeCriteria.getName()),
                    getRootTableVariable()),
                BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getHigh().intValue()))));
  }

  @VisibleForTesting
  public FunctionFilterVariable generateFilterForListCriteria(
      SnapshotBuilderProgramDataListCriteria listCriteria) {
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.IN,
        new FieldVariable(
            new FieldPointer(getRootTablePointer(), listCriteria.getName()),
            getRootTableVariable()),
        listCriteria.getValues().stream()
            .map(value -> new Literal(value.intValue()))
            .toArray(Literal[]::new));
  }

  private record OccurrenceTable(String tableName, String idColumnName) {}

  @VisibleForTesting
  public SubQueryFilterVariable generateFilterForDomainCriteria(
      SnapshotBuilderDomainCriteria domainCriteria) {
    OccurrenceTable occurrenceTable = getOccurrenceTableFromDomain(domainCriteria.getDomainName());

    TablePointer occurrencePointer =
        TablePointer.fromTableName(occurrenceTable.tableName(), tableNameGenerator);
    TableVariable occurrenceVariable = TableVariable.forPrimary(occurrencePointer);

    TablePointer ancestorPointer =
        TablePointer.fromTableName("concept_ancestor", tableNameGenerator);
    TableVariable ancestorVariable =
        TableVariable.forJoined(
            ancestorPointer,
            "ancestor_concept_id",
            new FieldVariable(
                new FieldPointer(occurrencePointer, occurrenceTable.idColumnName()),
                occurrenceVariable));

    return new SubQueryFilterVariable(
        new FieldVariable(
            new FieldPointer(getRootTablePointer(), "person_id"), getRootTableVariable()),
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
                        new Literal(domainCriteria.getId().intValue())),
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(ancestorPointer, "ancestor_concept_id"),
                            ancestorVariable),
                        BinaryFilterVariable.BinaryOperator.EQUALS,
                        new Literal(domainCriteria.getId().intValue()))))));
  }

  public FilterVariable generateFilterForCriteria(SnapshotBuilderCriteria criteria) {
    return switch (criteria.getKind()) {
      case LIST -> {
        if (criteria instanceof SnapshotBuilderProgramDataListCriteria) {
          yield generateFilterForListCriteria((SnapshotBuilderProgramDataListCriteria) criteria);
        }
        throw new BadRequestException("Malformed list criteria");
      }
      case RANGE -> {
        if (criteria instanceof SnapshotBuilderProgramDataRangeCriteria) {
          yield generateFilterForRangeCriteria((SnapshotBuilderProgramDataRangeCriteria) criteria);
        }
        throw new BadRequestException("Malformed range criteria");
      }
      case DOMAIN -> {
        if (criteria instanceof SnapshotBuilderDomainCriteria) {
          yield generateFilterForDomainCriteria((SnapshotBuilderDomainCriteria) criteria);
        }
        throw new BadRequestException("Malformed domain criteria");
      }
    };
  }

  public FilterVariable generateAndOrFilterForCriteriaGroup(
      SnapshotBuilderCriteriaGroup criteriaGroup) {
    return new BooleanAndOrFilterVariable(
        criteriaGroup.isMeetAll()
            ? BooleanAndOrFilterVariable.LogicalOperator.AND
            : BooleanAndOrFilterVariable.LogicalOperator.OR,
        criteriaGroup.getCriteria().stream()
            .map(this::generateFilterForCriteria)
            .collect(Collectors.toList()));
  }

  public FilterVariable generateFilterForCriteriaGroup(SnapshotBuilderCriteriaGroup criteriaGroup) {
    if (criteriaGroup.isMustMeet()) {
      return generateAndOrFilterForCriteriaGroup(criteriaGroup);
    } else {
      return new NotFilterVariable(generateAndOrFilterForCriteriaGroup(criteriaGroup));
    }
  }

  public FilterVariable generateFilterForCriteriaGroups(
      List<SnapshotBuilderCriteriaGroup> criteriaGroups) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        criteriaGroups.stream()
            .map(this::generateFilterForCriteriaGroup)
            .collect(Collectors.toList()));
  }

  public Query generateRollupCountsQueryForCriteriaGroupsList(
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {

    FieldVariable personId =
        new FieldVariable(
            new FieldPointer(getRootTablePointer(), "person_id", "COUNT"),
            getRootTableVariable(),
            "count",
            true);
    ;
    FilterVariable filterVariable =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.OR,
            criteriaGroupsList.stream()
                .map(this::generateFilterForCriteriaGroups)
                .collect(Collectors.toList()));

    return new Query(List.of(personId), List.of(getRootTableVariable()), filterVariable);
  }
}
