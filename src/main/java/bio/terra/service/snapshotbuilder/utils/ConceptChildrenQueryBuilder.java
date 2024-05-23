package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder.makeHasChildrenJoin;
import static bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder.selectHasChildren;

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
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptRelationship;
import java.util.List;

public class ConceptChildrenQueryBuilder {

  private static final String CONCEPT = "concept";
  private static final String CONCEPT_ANCESTOR = "concept_ancestor";
  private static final String CONCEPT_RELATIONSHIP = "concept_relationship";
  private static final String PERSON_ID = "person_id";
  private static final String DOMAIN_ID = "domain_id";
  private static final String CONCEPT_ID = "concept_id";
  private static final String CONCEPT_NAME = "concept_name";
  public static final String CONCEPT_CODE = "concept_code";
  private static final String STANDARD_CONCEPT = "standard_concept";
  private static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  private static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";
  private static final String CONCEPT_ID_1 = "concept_id_1";
  private static final String CONCEPT_ID_2 = "concept_id_2";
  private static final String RELATIONSHIP_ID = "relationship_id";

  /**
   * Generate a query that retrieves the descendants of the given concept and their roll-up counts.
   */
  public Query buildConceptChildrenQuery(
      SnapshotBuilderDomainOption domainOption, int parentConceptId) {

    // concept table and its fields concept_name and concept_id
    SourceVariable concept = SourceVariable.forPrimary(TablePointer.fromTableName(CONCEPT));
    FieldVariable conceptName = concept.makeFieldVariable(CONCEPT_NAME);
    FieldVariable conceptId = concept.makeFieldVariable(CONCEPT_ID);
    FieldVariable conceptCode = concept.makeFieldVariable(CONCEPT_CODE);

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id.
    // We use concept_ancestor for the rollup count because we want to include counts
    // from all descendants, not just direct descendants.
    SourceVariable conceptAncestor =
        SourceVariable.forJoined(
            TablePointer.fromTableName(CONCEPT_ANCESTOR), ANCESTOR_CONCEPT_ID, conceptId);
    FieldVariable descendantConceptId = conceptAncestor.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    var conceptRelationshipSubQuery = createConceptRelationshipSubQuery(parentConceptId);
    var conceptRelationshipSubqueryPointer =
        new SubQueryPointer(conceptRelationshipSubQuery, "concept_relationship_subquery");
    var conceptRelationship =
        SourceVariable.forJoined(
            conceptRelationshipSubqueryPointer, ConceptRelationship.CONCEPT_ID_2, conceptId);

    var joinHasChildren = makeHasChildrenJoin(conceptId);

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    SourceVariable domainOccurrence =
        SourceVariable.forLeftJoined(
            TablePointer.fromTableName(domainOption.getTableName()),
            domainOption.getColumnName(),
            descendantConceptId);

    // COUNT(DISTINCT person_id)
    FieldVariable count = domainOccurrence.makeFieldVariable(PERSON_ID, "COUNT", "count", true);

    List<SelectExpression> select =
        List.of(conceptName, conceptId, conceptCode, count, selectHasChildren(joinHasChildren));

    List<SourceVariable> tables =
        List.of(concept, conceptAncestor, conceptRelationship, joinHasChildren, domainOccurrence);

    List<FieldVariable> groupBy = List.of(conceptName, conceptId, conceptCode);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING));

    // c.standard_concept = 'S'
    FilterVariable where =
        BinaryFilterVariable.equals(concept.makeFieldVariable(STANDARD_CONCEPT), new Literal("S"));

    return new Query(select, tables, where, groupBy, orderBy);
  }

  /**
   * Generate a query that retrieves the descendants of the given concept. We use concept
   * relationship here because it includes only the direct descendants.
   *
   * <p>SELECT c.concept_id_2 FROM concept_relationship AS c WHERE (c.concept_id_1 = 101 AND
   * c.relationship_id = 'Subsumes'))
   */
  Query createConceptRelationshipSubQuery(int conceptId) {
    // concept_relationship is primary table for the subquery
    SourceVariable conceptRelationship =
        SourceVariable.forPrimary(TablePointer.fromTableName(CONCEPT_RELATIONSHIP));
    FieldVariable descendantConceptId = conceptRelationship.makeFieldVariable(CONCEPT_ID_2);

    return new Query(
        List.of(descendantConceptId),
        List.of(conceptRelationship),
        BooleanAndOrFilterVariable.and(
            BinaryFilterVariable.equals(
                conceptRelationship.makeFieldVariable(CONCEPT_ID_1), new Literal(conceptId)),
            BinaryFilterVariable.equals(
                conceptRelationship.makeFieldVariable(RELATIONSHIP_ID), new Literal("Subsumes"))));
  }

  /**
   * Generate a query that retrieves the domain id of the concept specified by the given conceptId.
   *
   * <p>SELECT c.domain_id FROM concept AS c WHERE c.concept_id = conceptId
   */
  public Query retrieveDomainId(int conceptId) {
    SourceVariable concept = SourceVariable.forPrimary(TablePointer.fromTableName(CONCEPT));
    FieldVariable domainIdField = concept.makeFieldVariable(DOMAIN_ID);

    BinaryFilterVariable where =
        new BinaryFilterVariable(
            concept.makeFieldVariable(CONCEPT_ID),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    List<SelectExpression> select = List.of(domainIdField);
    List<SourceVariable> table = List.of(concept);

    return new Query(select, table, where);
  }
}
