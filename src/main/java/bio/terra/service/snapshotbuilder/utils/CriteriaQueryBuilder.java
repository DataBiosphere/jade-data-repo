package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataOption;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.NotFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import bio.terra.service.snapshotbuilder.query.tables.ConceptAncestor;
import bio.terra.service.snapshotbuilder.query.tables.DomainOccurrence;
import bio.terra.service.snapshotbuilder.query.tables.Person;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class CriteriaQueryBuilder {
  final Person person = Person.asPrimary();

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

    DomainOccurrence domainOccurrence = DomainOccurrence.forPrimary(domainOption);

    ConceptAncestor conceptAncestor =
        ConceptAncestor.joinDescendant(domainOccurrence.getJoinColumn());
    return SubQueryFilterVariable.in(
        person.personId(),
        new Query.Builder()
            .select(List.of(domainOccurrence.getPerson()))
            .tables(List.of(domainOccurrence, conceptAncestor))
            .where(
                BinaryFilterVariable.equals(
                    conceptAncestor.ancestorConceptId(),
                    new Literal(domainCriteria.getConceptId())))
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

  public Query generateRollupCountsQueryForCohorts(List<SnapshotBuilderCohort> cohorts) {
    List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList =
        cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).toList();

    FieldVariable personId = person.countPersonId();

    return new Query.Builder()
        .select(List.of(personId))
        .tables(List.of(person))
        .where(generateFilterVariable(criteriaGroupsList))
        .build();
  }

  public Query generateRowIdQueryForCohorts(List<SnapshotBuilderCohort> cohorts) {
    List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList =
        cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).toList();
    FieldVariable rowId = person.rowId();

    // select row_id from person where the row is in the cohort specification
    return new Query.Builder()
        .select(List.of(rowId))
        .tables(List.of(person))
        .where(generateFilterVariable(criteriaGroupsList))
        .build();
  }

  @NotNull
  private FilterVariable generateFilterVariable(
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroupsList) {
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.OR,
        criteriaGroupsList.stream().map(this::generateFilterForCriteriaGroups).toList());
  }
}
