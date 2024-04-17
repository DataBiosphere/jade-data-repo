package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.exception.BadRequestException;
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
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.NotFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptAncestorConstants;
import bio.terra.service.snapshotbuilder.utils.constants.PersonConstants;
import java.util.List;
import java.util.Objects;

public class CriteriaQueryBuilder {
  final TableVariable rootTable;

  final SnapshotBuilderSettings snapshotBuilderSettings;

  protected CriteriaQueryBuilder(
      String rootTableName, SnapshotBuilderSettings snapshotBuilderSettings) {
    this.snapshotBuilderSettings = snapshotBuilderSettings;
    TablePointer tablePointer = TablePointer.fromTableName(rootTableName);
    rootTable = TableVariable.forPrimary(tablePointer);
  }

  private TablePointer getRootTablePointer() {
    return rootTable.getTablePointer();
  }

  private FieldVariable getFieldVariableForRootTable(String columnName) {
    return new FieldVariable(new FieldPointer(getRootTablePointer(), columnName), rootTable);
  }
  /**
   * Generate a filter for a SnapshotBuilderProgramDataRangeCriteria
   * <pre>{@code
   * p.year_of_birth >= low_value AND p.year_of_birth <= high_value
   * </pre>
   */
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

  /**
   * Generate filter for a SnapshotBuilderProgramDataListCriteria
   * <pre>{@code
   * if listCriteria has no values:
   *  sql = "1=1"
   *  else:
   *  "p.kind_of_list_criteria in ('values')
   * </pre>
   */
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

  /**
   * Generate filter for a SnapshotBuilderDomainCriteria
   * <pre>{@code
   *  p.person_id IN (SELECT c.person_id FROM 'domain'_occurrence AS c  JOIN concept_ancestor AS c0 ON c0.descendant_concept_id = c.'domain'_concept_id WHERE (c0.ancestor_concept_id = 'domainCriteria'.concept_id))"));
   * </pre>
   */
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

    TablePointer ancestorPointer =
        TablePointer.fromTableName(ConceptAncestorConstants.CONCEPT_ANCESTOR);
    TableVariable ancestorVariable =
        TableVariable.forJoined(
            ancestorPointer,
            ConceptAncestorConstants.DESCENDANT_CONCEPT_ID,
            new FieldVariable(
                new FieldPointer(occurrencePointer, domainOption.getColumnName()),
                occurrenceVariable));

    return new SubQueryFilterVariable(
        getFieldVariableForRootTable(PersonConstants.PERSON_ID),
        SubQueryFilterVariable.Operator.IN,
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(occurrencePointer, PersonConstants.PERSON_ID),
                    occurrenceVariable)),
            List.of(occurrenceVariable, ancestorVariable),
            new BooleanAndOrFilterVariable(
                BooleanAndOrFilterVariable.LogicalOperator.OR,
                List.of(
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(
                                ancestorPointer, ConceptAncestorConstants.ANCESTOR_CONCEPT_ID),
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
            new FieldPointer(getRootTablePointer(), PersonConstants.PERSON_ID, "COUNT"),
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
