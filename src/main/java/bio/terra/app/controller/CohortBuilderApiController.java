package bio.terra.app.controller;

import bio.terra.controller.CohortBuilderApi;
import bio.terra.model.Attribute;
import bio.terra.model.Cohort;
import bio.terra.model.CohortCreateInfo;
import bio.terra.model.CohortList;
import bio.terra.model.CohortUpdateInfo;
import bio.terra.model.ConceptSet;
import bio.terra.model.ConceptSetCreateInfo;
import bio.terra.model.ConceptSetList;
import bio.terra.model.ConceptSetUpdateInfo;
import bio.terra.model.CountQuery;
import bio.terra.model.Criteria;
import bio.terra.model.CriteriaGroup;
import bio.terra.model.DataType;
import bio.terra.model.DisplayHint;
import bio.terra.model.DisplayHintDisplayHint;
import bio.terra.model.DisplayHintEnum;
import bio.terra.model.DisplayHintEnumEnumHintValues;
import bio.terra.model.DisplayHintList;
import bio.terra.model.DisplayHintNumericRange;
import bio.terra.model.HintQuery;
import bio.terra.model.Instance;
import bio.terra.model.InstanceCount;
import bio.terra.model.InstanceCountList;
import bio.terra.model.InstanceHierarchyFields;
import bio.terra.model.InstanceList;
import bio.terra.model.InstanceRelationshipFields;
import bio.terra.model.Literal;
import bio.terra.model.LiteralValueUnion;
import bio.terra.model.Query;
import bio.terra.model.ValueDisplay;
import io.swagger.annotations.Api;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@Api(tags = {"cohort-builder"})
public class CohortBuilderApiController implements CohortBuilderApi {

  @Autowired
  public CohortBuilderApiController() {}

  @Override
  public ResponseEntity<CohortList> listCohorts(Integer offset, Integer limit) {
    CohortList response = new CohortList();
    response.add(generateStubCohort());
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Cohort> createCohort(CohortCreateInfo cohortCreateInfo) {
    return new ResponseEntity<>(generateStubCohort(), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Cohort> getCohort(String cohortId) {
    Cohort response = generateStubCohort();
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Cohort> updateCohort(String cohortId, CohortUpdateInfo cohortUpdateInfo) {
    return new ResponseEntity<>(generateStubCohort(), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> deleteCohort(String cohortId) {
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @Override
  public ResponseEntity<InstanceCountList> countInstances(
      String entityName, CountQuery countQuery) {

    return new ResponseEntity<>(
        new InstanceCountList()
            .sql("SQL goes here")
            .instanceCounts(
                createInstanceCountsYearGenderRace(
                    // Stub values based on what the API synthetic data returns in the other API -
                    // not representative of life
                    List.of(
                        new ValueDisplay()
                            .value(
                                new Literal()
                                    .dataType(DataType.INT64)
                                    .valueUnion(new LiteralValueUnion().int64Val(8507L)))
                            .display("MALE"),
                        new ValueDisplay()
                            .value(
                                new Literal()
                                    .dataType(DataType.INT64)
                                    .valueUnion(new LiteralValueUnion().int64Val(8532L)))
                            .display("FEMALE")),
                    List.of(
                        new ValueDisplay()
                            .value(
                                new Literal()
                                    .dataType(DataType.INT64)
                                    .valueUnion(new LiteralValueUnion().int64Val(0L)))
                            .display("No matching concept"),
                        new ValueDisplay()
                            .value(
                                new Literal()
                                    .dataType(DataType.INT64)
                                    .valueUnion(new LiteralValueUnion().int64Val(8516L)))
                            .display("Black or African American"),
                        new ValueDisplay()
                            .value(
                                new Literal()
                                    .dataType(DataType.INT64)
                                    .valueUnion(new LiteralValueUnion().int64Val(8527L)))
                            .display("White")),
                    LongStream.range(1909, 1984)
                        .mapToObj(
                            yearOfBirth ->
                                new ValueDisplay()
                                    .value(
                                        new Literal()
                                            .dataType(DataType.INT64)
                                            .valueUnion(
                                                new LiteralValueUnion().int64Val(yearOfBirth))))
                        .collect(Collectors.toList()))),
        HttpStatus.OK);
  }

  @Override
  public ResponseEntity<InstanceList> queryInstances(String entityName, Query query) {
    List<InstanceRelationshipFields> relationshipFields =
        List.of(
            new InstanceRelationshipFields()
                .relatedEntity("person")
                .hierarchy("standard")
                .count(1970071),
            new InstanceRelationshipFields().relatedEntity("person").count(0));

    List<InstanceHierarchyFields> hierarchyFields =
        List.of(new InstanceHierarchyFields().hierarchy("standard").path("").numChildren(29));

    Map<String, ValueDisplay> attributes =
        Map.of(
            "standard_concept",
                new ValueDisplay()
                    .display("Standard")
                    .value(
                        new Literal()
                            .dataType(DataType.STRING)
                            .valueUnion(new LiteralValueUnion().stringVal("S"))),
            "vocabulary",
                new ValueDisplay()
                    .display("Systematic Nomenclature of Medicine - Clinical Terms (IHTSDO)")
                    .value(
                        new Literal()
                            .dataType(DataType.STRING)
                            .valueUnion(new LiteralValueUnion().stringVal("SNOMED"))),
            "name",
                new ValueDisplay()
                    .value(
                        new Literal()
                            .dataType(DataType.STRING)
                            .valueUnion(new LiteralValueUnion().stringVal("Clinical finding"))),
            "concept_code",
                new ValueDisplay()
                    .value(
                        new Literal()
                            .dataType(DataType.STRING)
                            .valueUnion(new LiteralValueUnion().stringVal("404684003"))),
            "id",
                new ValueDisplay()
                    .value(
                        new Literal()
                            .dataType(DataType.INT64)
                            .valueUnion(new LiteralValueUnion().int64Val(441840L))));

    List<Instance> instances =
        List.of(
            new Instance()
                .relationshipFields(relationshipFields)
                .hierarchyFields(hierarchyFields)
                .attributes(attributes));

    return new ResponseEntity<>(
        new InstanceList().sql("SQL Goes here").instances(instances), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ConceptSetList> listConceptSets(
      @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
      @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
    ConceptSetList response = new ConceptSetList();
    response.add(generateStubConceptSet());
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ConceptSet> createConceptSet(ConceptSetCreateInfo conceptSetCreateInfo) {
    return new ResponseEntity<>(generateStubConceptSet(), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ConceptSet> getConceptSet(String ConceptSetId) {
    return new ResponseEntity<>(generateStubConceptSet(), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ConceptSet> updateConceptSet(
      String conceptSetId, ConceptSetUpdateInfo conceptSetUpdateInfo) {
    return new ResponseEntity<>(generateStubConceptSet(), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> deleteConceptSet(String conceptSetId) {
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @Override
  public ResponseEntity<DisplayHintList> queryHints(String entityId, HintQuery hintQuery) {
    DisplayHintList response =
        new DisplayHintList()
            .addDisplayHintsItem(
                new DisplayHint()
                    .attribute(
                        new Attribute()
                            .name("gender")
                            .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                            .dataType(DataType.INT64))
                    .displayHint(
                        new DisplayHintDisplayHint()
                            .enumHint(
                                new DisplayHintEnum()
                                    .enumHintValues(
                                        List.of(
                                            new DisplayHintEnumEnumHintValues()
                                                .enumVal(
                                                    new ValueDisplay()
                                                        .display("FEMALE")
                                                        .value(
                                                            new Literal()
                                                                .dataType(DataType.INT64)
                                                                .valueUnion(
                                                                    new LiteralValueUnion()
                                                                        .int64Val(8532L))))
                                                .count(1292861),
                                            new DisplayHintEnumEnumHintValues()
                                                .enumVal(
                                                    new ValueDisplay()
                                                        .display("MALE")
                                                        .value(
                                                            new Literal()
                                                                .dataType(DataType.INT64)
                                                                .valueUnion(
                                                                    new LiteralValueUnion()
                                                                        .int64Val(8507L))))
                                                .count(1033995))))))
            .addDisplayHintsItem(
                new DisplayHint()
                    .attribute(
                        new Attribute()
                            .name("race")
                            .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                            .dataType(DataType.INT64))
                    .displayHint(
                        new DisplayHintDisplayHint()
                            .enumHint(
                                new DisplayHintEnum()
                                    .enumHintValues(
                                        Arrays.asList(
                                            new DisplayHintEnumEnumHintValues()
                                                .enumVal(
                                                    new ValueDisplay()
                                                        .display("Black or African American")
                                                        .value(
                                                            new Literal()
                                                                .dataType(DataType.INT64)
                                                                .valueUnion(
                                                                    new LiteralValueUnion()
                                                                        .int64Val(8516L))))
                                                .count(247723),
                                            new DisplayHintEnumEnumHintValues()
                                                .enumVal(
                                                    new ValueDisplay()
                                                        .display("No matching concept")
                                                        .value(
                                                            new Literal()
                                                                .dataType(DataType.INT64)
                                                                .valueUnion(
                                                                    new LiteralValueUnion()
                                                                        .int64Val(0L))))
                                                .count(152425),
                                            new DisplayHintEnumEnumHintValues()
                                                .enumVal(
                                                    new ValueDisplay()
                                                        .display("White")
                                                        .value(
                                                            new Literal()
                                                                .dataType(DataType.INT64)
                                                                .valueUnion(
                                                                    new LiteralValueUnion()
                                                                        .int64Val(8527L))))
                                                .count(1926708))))))
            .addDisplayHintsItem(
                new DisplayHint()
                    .attribute(
                        new Attribute()
                            .name("ethnicity")
                            .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                            .dataType(DataType.INT64))
                    .displayHint(
                        new DisplayHintDisplayHint()
                            .enumHint(
                                new DisplayHintEnum()
                                    .enumHintValues(
                                        Arrays.asList(
                                            new DisplayHintEnumEnumHintValues()
                                                .enumVal(
                                                    new ValueDisplay()
                                                        .display("Hispanic or Latino")
                                                        .value(
                                                            new Literal()
                                                                .dataType(DataType.INT64)
                                                                .valueUnion(
                                                                    new LiteralValueUnion()
                                                                        .int64Val(38003563L))))
                                                .count(54453),
                                            new DisplayHintEnumEnumHintValues()
                                                .enumVal(
                                                    new ValueDisplay()
                                                        .display("Not Hispanic or Latino")
                                                        .value(
                                                            new Literal()
                                                                .dataType(DataType.INT64)
                                                                .valueUnion(
                                                                    new LiteralValueUnion()
                                                                        .int64Val(38003564L))))
                                                .count(2272403))))))
            .addDisplayHintsItem(
                new DisplayHint()
                    .attribute(
                        new Attribute()
                            .name("year_of_birth")
                            .type(Attribute.TypeEnum.SIMPLE)
                            .dataType(DataType.INT64))
                    .displayHint(
                        new DisplayHintDisplayHint()
                            .numericRangeHint(
                                new DisplayHintNumericRange().min(1909.0D).max(1983.0D))));

    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  // Our stubs have the same count for everything
  private List<InstanceCount> createInstanceCountsYearGenderRace(
      List<ValueDisplay> genders, List<ValueDisplay> races, List<ValueDisplay> yearsOfBirth) {
    record GR(ValueDisplay gender, ValueDisplay race) {}
    record GRY(ValueDisplay gender, ValueDisplay race, ValueDisplay year) {}

    return genders.stream()
        .flatMap(gender -> races.stream().map(race -> new GR(gender, race)))
        .flatMap(
            gr ->
                yearsOfBirth.stream().map(yearOfBirth -> new GRY(gr.gender, gr.race, yearOfBirth)))
        .map(
            gry ->
                new InstanceCount()
                    .count(20000)
                    .attributes(
                        Map.of("gender", gry.gender, "race", gry.race, "year_of_birth", gry.year)))
        .collect(Collectors.toList());
  }

  private Cohort generateStubCohort() {
    return new Cohort()
        .id("42VHg43VxA")
        .displayName("My Cohort")
        .criteriaGroups(
            List.of(
                new CriteriaGroup()
                    .id("A3FJlc87")
                    .displayName("")
                    .criteria(
                        List.of(
                            new Criteria()
                                .id("Q1Ng787e")
                                .displayName("")
                                .pluginName("classification")
                                .selectionData(
                                    "{\"selected\":[{\"key\":4047779,\"name\":\"Disorder by body site\"}]}")
                                .uiConfig(
                                    "{\"type\":\"classification\",\"id\":\"tanagra-conditions\",\"title\":\"Condition\",\"conceptSet\":true,\"category\":\"Domains\",\"columns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Concept name\"},{\"key\":\"id\",\"width\":100,\"title\":\"Concept ID\"},{\"key\":\"standard_concept\",\"width\":120,\"title\":\"Source/standard\"},{\"key\":\"vocabulary_t_value\",\"width\":120,\"title\":\"Vocab\"},{\"key\":\"concept_code\",\"width\":120,\"title\":\"Code\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"hierarchyColumns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Condition\"},{\"key\":\"id\",\"width\":120,\"title\":\"Concept ID\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"occurrence\":\"condition_occurrence\",\"classification\":\"condition\"}")))));
  }

  private ConceptSet generateStubConceptSet() {
    return new ConceptSet()
        .id("Mu6OSPzPdp")
        .entity("condition_occurrence")
        .created(OffsetDateTime.now())
        .lastModified(OffsetDateTime.now())
        .criteria(
            new Criteria()
                .id("DrX3z0V9")
                .displayName("")
                .pluginName("classification")
                .selectionData("{\"selected\":[{\"key\":441840,\"name\":\"Clinical finding\"}]}")
                .uiConfig(
                    "{\"type\":\"classification\",\"id\":\"tanagra-conditions\",\"title\":\"Condition\",\"conceptSet\":true,\"category\":\"Domains\",\"columns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Concept name\"},{\"key\":\"id\",\"width\":100,\"title\":\"Concept ID\"},{\"key\":\"standard_concept\",\"width\":120,\"title\":\"Source/standard\"},{\"key\":\"vocabulary_t_value\",\"width\":120,\"title\":\"Vocab\"},{\"key\":\"concept_code\",\"width\":120,\"title\":\"Code\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"hierarchyColumns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Condition\"},{\"key\":\"id\",\"width\":120,\"title\":\"Concept ID\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"occurrence\":\"condition_occurrence\",\"classification\":\"condition\"}"))
        .createdBy("authentication-disabled");
  }
}
