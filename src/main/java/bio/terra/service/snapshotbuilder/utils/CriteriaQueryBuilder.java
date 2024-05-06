package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.snapshotbuilder.query.ConceptAncestor;
import bio.terra.service.snapshotbuilder.query.ConditionOccurrence;
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
import bio.terra.service.snapshotbuilder.query.TableVariableBuilder;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import java.util.List;
import java.util.Objects;

public class CriteriaQueryBuilder {
  final TableVariable rootTable;

  final SnapshotBuilderSettings snapshotBuilderSettings;

  protected CriteriaQueryBuilder(SnapshotBuilderSettings snapshotBuilderSettings) {
    this.snapshotBuilderSettings = snapshotBuilderSettings;
    rootTable = TableVariable.forPrimary(TablePointer.fromTableName("person"));
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
    TableVariable occurrenceVariable = TableVariable.forPrimary(occurrencePointer);

    ConceptAncestor conceptAncestor =
        new ConceptAncestor(
            new TableVariableBuilder()
                .join(ConceptAncestor.DESCENDANT_CONCEPT_ID)
                .on(
                    new FieldVariable(
                        new FieldPointer(occurrencePointer, domainOption.getColumnName()),
                        occurrenceVariable)));
    return SubQueryFilterVariable.in(
        getFieldVariableForRootTable(Person.PERSON_ID),
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(occurrencePointer, ConditionOccurrence.PERSON_ID),
                    occurrenceVariable)),
            List.of(occurrenceVariable, conceptAncestor),
            BinaryFilterVariable.equals(
                conceptAncestor.ancestor_concept_id(),
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
            new FieldPointer(getRootTablePointer(), Person.PERSON_ID, "COUNT"),
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
