package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SourcePointer;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.NotFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptAncestor;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class CriteriaQueryBuilder {
  final SourceVariable rootTable;

  final SnapshotBuilderSettings snapshotBuilderSettings;

  protected CriteriaQueryBuilder(
      String rootTableName, SnapshotBuilderSettings snapshotBuilderSettings) {
    this.snapshotBuilderSettings = snapshotBuilderSettings;
    TablePointer tablePointer = TablePointer.fromTableName(rootTableName);
    rootTable = SourceVariable.forPrimary(tablePointer);
  }

  private SourcePointer getRootTablePointer() {
    return rootTable.getSourcePointer();
  }

  private FieldVariable getFieldVariableForRootTable(String columnName) {
    return new FieldVariable(new FieldPointer(getRootTablePointer(), columnName), rootTable);
  }

  FilterVariable generateFilter(SnapshotBuilderProgramDataRangeCriteria rangeCriteria) {
    FieldVariable rangeVariable =
        getFieldVariableForRootTable(getProgramDataOptionColumnName(rangeCriteria.getId()));
    return BooleanAndOrFilterVariable.and(
        new BinaryFilterVariable(
            rangeVariable,
            BinaryFilterVariable.BinaryOperator.GREATER_THAN_OR_EQUAL,
            new Literal(rangeCriteria.getLow())),
        new BinaryFilterVariable(
            rangeVariable,
            BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL,
            new Literal(rangeCriteria.getHigh())));
  }

  FilterVariable generateFilter(SnapshotBuilderProgramDataListCriteria listCriteria) {
    if (listCriteria.getValues().isEmpty()) {
      // select all values of list criteria
      return FilterVariable.alwaysTrueFilter();
    }
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.IN,
        getFieldVariableForRootTable(getProgramDataOptionColumnName(listCriteria.getId())),
        listCriteria.getValues().stream().map(Literal::new).toArray(Literal[]::new));
  }

  String getProgramDataOptionColumnName(int id) {
    return snapshotBuilderSettings.getProgramDataOptions().stream()
        .filter(programDataOption -> Objects.equals(programDataOption.getId(), id))
        .findFirst()
        .orElseThrow(
            () -> new BadRequestException(String.format("Invalid program data ID given: %d", id)))
        .getColumnName();
  }

  FilterVariable generateFilter(SnapshotBuilderDomainCriteria domainCriteria) {
    SnapshotBuilderDomainOption domainOption =
        snapshotBuilderSettings.getDomainOptions().stream()
            .filter(
                snapshotBuilderDomainOption ->
                    Objects.equals(snapshotBuilderDomainOption.getId(), domainCriteria.getId()))
            .findFirst()
            .orElseThrow(
                () ->
                    new BadRequestException(
                        String.format(
                            "Domain %d not configured for use in Snapshot Builder",
                            domainCriteria.getId())));

    TablePointer occurrencePointer = TablePointer.fromTableName(domainOption.getTableName());
    SourceVariable occurrenceVariable = SourceVariable.forPrimary(occurrencePointer);

    TablePointer ancestorPointer = TablePointer.fromTableName(ConceptAncestor.TABLE_NAME);
    SourceVariable ancestorVariable =
        SourceVariable.forJoined(
            ancestorPointer,
            ConceptAncestor.DESCENDANT_CONCEPT_ID,
            new FieldVariable(
                new FieldPointer(occurrencePointer, domainOption.getColumnName()),
                occurrenceVariable));

    return SubQueryFilterVariable.in(
        getFieldVariableForRootTable(Person.PERSON_ID),
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(occurrencePointer, ConditionOccurrence.PERSON_ID),
                    occurrenceVariable)),
            List.of(occurrenceVariable, ancestorVariable),
            BinaryFilterVariable.equals(
                new FieldVariable(
                    new FieldPointer(ancestorPointer, ConceptAncestor.ANCESTOR_CONCEPT_ID),
                    ancestorVariable),
                new Literal(domainCriteria.getConceptId()))));
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
        criteriaGroup.isMeetAll()
            ? BooleanAndOrFilterVariable.LogicalOperator.AND
            : BooleanAndOrFilterVariable.LogicalOperator.OR,
        criteriaGroup.getCriteria().stream().map(this::generateFilterForCriteria).toList());
  }

  FilterVariable generateFilterForCriteriaGroup(SnapshotBuilderCriteriaGroup criteriaGroup) {
    FilterVariable andOrFilterVariable = generateAndOrFilterForCriteriaGroup(criteriaGroup);
    return criteriaGroup.isMustMeet()
        ? andOrFilterVariable
        : new NotFilterVariable(andOrFilterVariable);
  }

  FilterVariable generateFilterForCriteriaGroups(
      List<SnapshotBuilderCriteriaGroup> criteriaGroups) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        criteriaGroups.stream().map(this::generateFilterForCriteriaGroup).toList());
  }

  public Query generateRollupCountsQueryForCohorts(List<SnapshotBuilderCohort> cohorts) {
    List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList =
        cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).toList();

    FieldVariable personId =
        new FieldVariable(
            new FieldPointer(getRootTablePointer(), Person.PERSON_ID, "COUNT"),
            rootTable,
            null,
            true);

    return new Query(
        List.of(personId), List.of(rootTable), generateFilterVariable(criteriaGroupsList));
  }

  public Query generateRowIdQueryForCohorts(List<SnapshotBuilderCohort> cohorts) {
    List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList =
        cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).toList();
    FieldVariable rowId =
        new FieldVariable(new FieldPointer(getRootTablePointer(), Person.ROW_ID), rootTable);

    // select row_id from person where the row is in the cohort specification
    return new Query(
        List.of(rowId), List.of(rootTable), generateFilterVariable(criteriaGroupsList));
  }

  @NotNull
  private FilterVariable generateFilterVariable(
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.OR,
        criteriaGroupsList.stream().map(this::generateFilterForCriteriaGroups).toList());
  }
}
