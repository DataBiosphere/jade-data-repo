package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder.makeHasChildrenJoin;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.SubQueryPointer;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import bio.terra.service.snapshotbuilder.query.table.ConceptAncestor;
import bio.terra.service.snapshotbuilder.query.table.ConceptRelationship;
import bio.terra.service.snapshotbuilder.query.table.DomainOccurrence;
import bio.terra.service.snapshotbuilder.query.table.Table;
import java.util.List;

public class ConceptChildrenQueryBuilder {
  /**
   * Generate a query that retrieves the descendants of the given concept and their roll-up counts.
   */
  public Query buildConceptChildrenQuery(
      SnapshotBuilderDomainOption domainOption, int parentConceptId) {

    // concept table and its fields concept_name and concept_id
    Concept concept = Concept.asPrimary();
    FieldVariable conceptName = concept.name();
    FieldVariable conceptId = concept.conceptId();
    FieldVariable conceptCode = concept.conceptCode();

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id.
    // We use concept_ancestor for the rollup count because we want to include counts
    // from all descendants, not just direct descendants.
    ConceptAncestor conceptAncestor = ConceptAncestor.joinAncestor(conceptId);

    // Join with concept relationship table to get the direct children of the given concept id
    var conceptRelationshipSubQuery = createConceptRelationshipSubQuery(parentConceptId);
    var conceptRelationshipSubqueryPointer =
        new SubQueryPointer(conceptRelationshipSubQuery, "concept_relationship_subquery");
    SourceVariable conceptRelationship =
        SourceVariable.forJoined(
            conceptRelationshipSubqueryPointer, ConceptRelationship.CONCEPT_ID_2, conceptId);

    SourceVariable joinHasChildren = makeHasChildrenJoin(conceptId);

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    DomainOccurrence domainOccurrence =
        DomainOccurrence.leftJoinOn(domainOption, conceptAncestor.descendantConceptId());

    // COUNT(DISTINCT person_id)
    FieldVariable countPerson = domainOccurrence.countPerson();

    List<SelectExpression> select =
        List.of(
            conceptName,
            conceptId,
            conceptCode,
            countPerson,
            ConceptAncestor.selectHasChildren(joinHasChildren));

    List<Table> tables =
        List.of(
            concept,
            conceptAncestor,
            new Table(conceptRelationship),
            new Table(joinHasChildren),
            domainOccurrence);

    List<FieldVariable> groupBy = List.of(conceptName, conceptId, conceptCode);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING));

    // c.standard_concept = 'S'
    FilterVariable where = BinaryFilterVariable.equals(concept.standardConcept(), new Literal("S"));

    return new Query.Builder()
        .select(select)
        .tables(tables)
        .where(where)
        .groupBy(groupBy)
        .orderBy(orderBy)
        .build();
  }

  /**
   * Generate a query that retrieves the descendants of the given concept. We use concept
   * relationship here because it includes only the direct descendants.
   */
  static Query createConceptRelationshipSubQuery(int conceptId) {
    ConceptRelationship conceptRelationship = ConceptRelationship.forPrimary();
    FieldVariable descendantConceptId = conceptRelationship.getConceptId2();

    return new Query.Builder()
        .select(List.of(descendantConceptId))
        .tables(List.of(conceptRelationship))
        .where(
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(
                    conceptRelationship.getConceptId1(), new Literal(conceptId)),
                BinaryFilterVariable.equals(
                    conceptRelationship.relationshipId(), new Literal("Subsumes"))))
        .build();
  }

  /**
   * Generate a query that retrieves the domain id of the concept specified by the given conceptId.
   *
   * <p>SELECT c.domain_id FROM concept AS c WHERE c.concept_id = conceptId
   */
  public Query retrieveDomainId(int conceptId) {
    Concept concept = Concept.asPrimary();
    FieldVariable domainId = concept.domainId();

    BinaryFilterVariable where =
        new BinaryFilterVariable(
            concept.conceptId(),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    return new Query.Builder()
        .select(List.of(domainId))
        .tables(List.of(concept))
        .where(where)
        .build();
  }
}
