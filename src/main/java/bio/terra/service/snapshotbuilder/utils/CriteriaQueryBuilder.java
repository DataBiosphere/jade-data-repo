package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderColumn;
import bio.terra.model.SnapshotBuilderColumnFilterDetails;
import bio.terra.model.SnapshotBuilderColumnFilterType;
import bio.terra.model.SnapshotBuilderColumnSelectDetails;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderPrimaryTable;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataOption;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.NotFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import bio.terra.service.snapshotbuilder.query.table.Person;
import bio.terra.service.snapshotbuilder.query.table.Table;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class CriteriaQueryBuilder {
  final Person person = Person.forPrimary();

  final SnapshotBuilderSettings snapshotBuilderSettings;

  protected CriteriaQueryBuilder(SnapshotBuilderSettings snapshotBuilderSettings) {
    this.snapshotBuilderSettings = snapshotBuilderSettings;
  }

  FilterVariable generateFilter(SnapshotBuilderProgramDataRangeCriteria rangeCriteria) {
    FieldVariable rangeVariable =
        person.variableForOption(getProgramDataOptionColumnName(rangeCriteria.getId()));
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
        person.variableForOption(getProgramDataOptionColumnName(listCriteria.getId())),
        listCriteria.getValues().stream().map(Literal::new).toArray(Literal[]::new));
  }

  SnapshotBuilderProgramDataOption getProgramDataOptionColumnName(int id) {
    return snapshotBuilderSettings.getProgramDataOptions().stream()
        .filter(programDataOption -> Objects.equals(programDataOption.getId(), id))
        .findFirst()
        .orElseThrow(
            () -> new BadRequestException(String.format("Invalid program data ID given: %d", id)));
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
    List<Table> tables = new java.util.ArrayList<>(List.of());
    List<SelectExpression> select = new java.util.ArrayList<>(List.of());
    List<FilterVariable> searchTermFilterVariables = new java.util.ArrayList<>(List.of());

    SnapshotBuilderPrimaryTable snapshotBuilderPrimaryTable =
        domainOption.getRows().getStartsWith();
    Table primaryTable = Table.asPrimary(snapshotBuilderPrimaryTable.getName());
    tables.add(primaryTable);
    addColumnInformation(
        snapshotBuilderPrimaryTable.getColumnsUsed(),
        primaryTable,
        select,
        searchTermFilterVariables,
        domainCriteria.getConceptId());
    domainOption
        .getRows()
        .getJoinTables()
        .forEach(
            snapshotBuilderJoinModel -> {
              FieldVariable joinOn =
                  tables.stream()
                      .filter(
                          table ->
                              Objects.equals(
                                  table.tableName(), snapshotBuilderJoinModel.getFrom().getTable()))
                      .findFirst()
                      .orElseThrow()
                      .getFieldVariable(snapshotBuilderJoinModel.getFrom().getColumn());
              Table joinTable =
                  Table.asJoined(
                      snapshotBuilderJoinModel.getTo().getTable(),
                      snapshotBuilderJoinModel.getTo().getColumn(),
                      joinOn,
                      snapshotBuilderJoinModel.isLeftJoin());
              tables.add(joinTable);
              addColumnInformation(
                  snapshotBuilderJoinModel.getTo().getColumnsUsed(),
                  joinTable,
                  select,
                  searchTermFilterVariables,
                  domainCriteria.getConceptId());
            });

    return SubQueryFilterVariable.in(
        person.personId(),
        new Query.Builder()
            .select(select)
            .tables(tables)
            .where(BooleanAndOrFilterVariable.and(searchTermFilterVariables))
            .build());
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

  public Query runSingleFieldQuery(
      List<SnapshotBuilderCohort> cohorts, FieldVariable fieldVariable) {
    List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList =
        cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).toList();

    return new Query.Builder()
        .select(List.of(fieldVariable))
        .tables(List.of(person))
        .where(generateFilterVariable(criteriaGroupsList))
        .build();
  }

  public Query generateRollupCountsQueryForCohorts(List<SnapshotBuilderCohort> cohorts) {
    return runSingleFieldQuery(cohorts, person.countPerson());
  }

  public Query generateRowIdQueryForCohorts(List<SnapshotBuilderCohort> cohorts) {
    return runSingleFieldQuery(cohorts, person.rowId());
  }

  public void addColumnInformation(
      List<SnapshotBuilderColumn> columns,
      Table table,
      List<SelectExpression> select,
      List<FilterVariable> searchTermFilterVariables,
      Integer id) {
    columns.forEach(
        snapshotBuilderColumn -> {
          SnapshotBuilderColumnSelectDetails selectDetails =
              snapshotBuilderColumn.getSelectDetails();
          FieldVariable fieldVariable = table.getFieldVariable(snapshotBuilderColumn.getName());
          if (snapshotBuilderColumn.getSelectDetails() != null) {
            fieldVariable =
                table.getFieldVariable(
                    snapshotBuilderColumn.getName(),
                    selectDetails.getFunctionWrapper(),
                    selectDetails.getAlias(),
                    selectDetails.isIsDistinct());
            select.add(fieldVariable);
          }
          List<SnapshotBuilderColumnFilterDetails> filterDetails =
              snapshotBuilderColumn.getFilterDetails();
          if (snapshotBuilderColumn.getFilterDetails() != null) {
            FieldVariable filterFieldVariable = fieldVariable;
            filterDetails.forEach(
                filterDetail -> {
                  if (Objects.requireNonNull(filterDetail.getType())
                      == SnapshotBuilderColumnFilterType.SEARCH_TERM) {
                    searchTermFilterVariables.add(
                        createFilterConceptClause(filterFieldVariable, id));
                  }
                });
          }
        });
  }

  static BinaryFilterVariable createFilterConceptClause(FieldVariable fieldVariable, Integer id) {
    return BinaryFilterVariable.equals(fieldVariable, new Literal(id));
  }

  @NotNull
  private FilterVariable generateFilterVariable(
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.OR,
        criteriaGroupsList.stream().map(this::generateFilterForCriteriaGroups).toList());
  }
}
