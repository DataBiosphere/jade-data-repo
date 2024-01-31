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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CriteriaQueryBuilder {
  public static final String PERSON_ID_FIELD_NAME = "person_id";
  private final TableNameGenerator tableNameGenerator;
  final TableVariable rootTable;

  private record OccurrenceTable(String tableName, String idColumnName) {}

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

  public CriteriaQueryBuilder(String rootTableName, TableNameGenerator tableNameGenerator) {
    this.tableNameGenerator = tableNameGenerator;
    TablePointer tablePointer = TablePointer.fromTableName(rootTableName, tableNameGenerator);
    rootTable = TableVariable.forPrimary(tablePointer);
  }

  private TablePointer getRootTablePointer() {
    return rootTable.getTablePointer();
  }

  private FieldVariable getFieldVariableForRootTable(String columnName) {
    return new FieldVariable(new FieldPointer(getRootTablePointer(), columnName), rootTable);
  }

  FilterVariable generateFilter(SnapshotBuilderProgramDataRangeCriteria rangeCriteria) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        List.of(
            new BinaryFilterVariable(
                getFieldVariableForRootTable(rangeCriteria.getName()),
                BinaryFilterVariable.BinaryOperator.GREATER_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getLow().intValue())),
            new BinaryFilterVariable(
                getFieldVariableForRootTable(rangeCriteria.getName()),
                BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getHigh().intValue()))));
  }

  FilterVariable generateFilter(SnapshotBuilderProgramDataListCriteria listCriteria) {
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.IN,
        getFieldVariableForRootTable(listCriteria.getName()),
        listCriteria.getValues().stream()
            .map(value -> new Literal(value.intValue()))
            .toArray(Literal[]::new));
  }

  FilterVariable generateFilter(SnapshotBuilderDomainCriteria domainCriteria) {
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
        getFieldVariableForRootTable(PERSON_ID_FIELD_NAME),
        SubQueryFilterVariable.Operator.IN,
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(occurrencePointer, PERSON_ID_FIELD_NAME), occurrenceVariable)),
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

  FilterVariable generateFilterForCriteria(SnapshotBuilderCriteria criteria) {
    return switch (criteria.getKind()) {
      case LIST -> generateFilter((SnapshotBuilderProgramDataListCriteria) criteria);
      case RANGE -> generateFilter((SnapshotBuilderProgramDataRangeCriteria) criteria);
      case DOMAIN -> generateFilter((SnapshotBuilderDomainCriteria) criteria);
    };
  }

  FilterVariable generateAndOrFilterForCriteriaGroup(SnapshotBuilderCriteriaGroup criteriaGroup) {
    return new BooleanAndOrFilterVariable(
        Objects.requireNonNullElse(criteriaGroup.isMeetAll(), false)
            ? BooleanAndOrFilterVariable.LogicalOperator.AND
            : BooleanAndOrFilterVariable.LogicalOperator.OR,
        criteriaGroup.getCriteria().stream().map(this::generateFilterForCriteria).toList());
  }

  FilterVariable generateFilterForCriteriaGroup(SnapshotBuilderCriteriaGroup criteriaGroup) {
    FilterVariable andOrFilterVariable = generateAndOrFilterForCriteriaGroup(criteriaGroup);
    return Objects.requireNonNullElse(criteriaGroup.isMeetAll(), false)
        ? andOrFilterVariable
        : new NotFilterVariable(andOrFilterVariable);
  }

  FilterVariable generateFilterForCriteriaGroups(
      List<SnapshotBuilderCriteriaGroup> criteriaGroups) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        criteriaGroups.stream().map(this::generateFilterForCriteriaGroup).toList());
  }

  public Query generateRollupCountsQueryForCriteriaGroupsList(
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {

    FieldVariable personId =
        new FieldVariable(
            new FieldPointer(getRootTablePointer(), PERSON_ID_FIELD_NAME, "COUNT"),
            rootTable,
            "count",
            true);

    FilterVariable filterVariable =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.OR,
            criteriaGroupsList.stream().map(this::generateFilterForCriteriaGroups).toList());

    return new Query(List.of(personId), List.of(rootTable), filterVariable);
  }
}
