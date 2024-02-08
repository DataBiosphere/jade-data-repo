package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderSettings;
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

  final SnapshotBuilderSettings snapshotBuilderSettings;

  private record OccurrenceTable(String tableName, String idColumnName) {}

  private static final Map<Integer, OccurrenceTable> DOMAIN_TO_OCCURRENCE_TABLE =
      Map.of(
          19, new OccurrenceTable("condition_occurrence", "condition_concept_id"),
          10, new OccurrenceTable("procedure_occurrence", "procedure_concept_id"),
          27, new OccurrenceTable("observation", "observation_concept_id"),
          17, new OccurrenceTable("device_exposure", "device_concept_id"),
          13, new OccurrenceTable("drug_exposure", "drug_concept_id"));

  private static OccurrenceTable getOccurrenceTableFromDomain(Integer domain) {
    OccurrenceTable occurrenceTable = DOMAIN_TO_OCCURRENCE_TABLE.get(domain);
    if (occurrenceTable == null) {
      throw new BadRequestException(String.format("Domain %s is not found in dataset", domain));
    }
    return occurrenceTable;
  }

  protected CriteriaQueryBuilder(
      String rootTableName,
      TableNameGenerator tableNameGenerator,
      SnapshotBuilderSettings snapshotBuilderSettings) {
    this.tableNameGenerator = tableNameGenerator;
    this.snapshotBuilderSettings = snapshotBuilderSettings;
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
    String columnName = getProgramDataOptionColumnName(rangeCriteria.getId());
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        List.of(
            new BinaryFilterVariable(
                getFieldVariableForRootTable(columnName),
                BinaryFilterVariable.BinaryOperator.GREATER_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getLow())),
            new BinaryFilterVariable(
                getFieldVariableForRootTable(columnName),
                BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getHigh()))));
  }

  FilterVariable generateFilter(SnapshotBuilderProgramDataListCriteria listCriteria) {
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.IN,
        getFieldVariableForRootTable(getProgramDataOptionColumnName(listCriteria.getId())),
        listCriteria.getValues().stream()
            .map(Literal::new)
            .toArray(Literal[]::new));
  }

  String getProgramDataOptionColumnName(Integer id) {
    return snapshotBuilderSettings.getProgramDataOptions().stream()
        .filter(programDataOption -> Objects.equals(programDataOption.getId(), id))
        .findFirst()
        .orElseThrow()
        .getColumnName();
  }

  FilterVariable generateFilter(SnapshotBuilderDomainCriteria domainCriteria) {
    OccurrenceTable occurrenceTable = getOccurrenceTableFromDomain(domainCriteria.getId());

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
                        new Literal(domainCriteria.getConceptId())),
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(ancestorPointer, "ancestor_concept_id"),
                            ancestorVariable),
                        BinaryFilterVariable.BinaryOperator.EQUALS,
                        new Literal(domainCriteria.getConceptId()))))));
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
    return Objects.requireNonNullElse(criteriaGroup.isMustMeet(), true)
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
            null,
            true);

    FilterVariable filterVariable =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.OR,
            criteriaGroupsList.stream().map(this::generateFilterForCriteriaGroups).toList());

    return new Query(List.of(personId), List.of(rootTable), filterVariable);
  }
}
