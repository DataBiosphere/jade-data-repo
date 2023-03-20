package bio.terra.service.cohortbuilder;

import bio.terra.model.CountQuery;
import bio.terra.model.DataType;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.springframework.stereotype.Component;

@Component
public class InstancesService {

  public InstanceCountList countInstances(String entityName, CountQuery countQuery) {
    return new InstanceCountList()
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
                                        .valueUnion(new LiteralValueUnion().int64Val(yearOfBirth))))
                    .collect(Collectors.toList())));
  }

  public InstanceList queryInstances(String entityName, Query query) {
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
    return new InstanceList().sql("SQL Goes here").instances(instances);
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
}
