package bio.terra.service.snapshotbuilder;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderDatasetConceptSets;
import bio.terra.model.SnapshotBuilderDomain;
import bio.terra.model.SnapshotBuilderFeatureValueGroup;
import bio.terra.model.SnapshotBuilderProgramData;
import bio.terra.model.SnapshotBuilderSettings;
import java.util.List;

public class SnapshotBuilderTestData {
  public static final SnapshotBuilderSettings SETTINGS =
      new SnapshotBuilderSettings()
          .selectableDomains(
              List.of(
                  new SnapshotBuilderDomain()
                      .id(10)
                      .category("Condition")
                      .root(
                          new SnapshotBuilderConcept()
                              .id(100)
                              .name("Condition")
                              .count(100)
                              .hasChildren(true)),
                  new SnapshotBuilderDomain()
                      .id(11)
                      .category("Procedure")
                      .root(
                          new SnapshotBuilderConcept()
                              .id(200)
                              .name("Procedure")
                              .count(100)
                              .hasChildren(true)),
                  new SnapshotBuilderDomain()
                      .id(12)
                      .category("Observation")
                      .root(
                          new SnapshotBuilderConcept()
                              .id(300)
                              .name("Observation")
                              .count(100)
                              .hasChildren(true))))
          .selectableProgramData(
              List.of(
                  new SnapshotBuilderProgramData()
                      .id(1)
                      .name("Year of birth")
                      .kind(SnapshotBuilderProgramData.KindEnum.RANGE)
                      .columnName("year_of_birth"),
                  new SnapshotBuilderProgramData()
                      .id(2)
                      .name("Ethnicity")
                      .kind(SnapshotBuilderProgramData.KindEnum.LIST)
                      .columnName("ethnicity"),
                  new SnapshotBuilderProgramData()
                      .id(3)
                      .name("Gender identity")
                      .kind(SnapshotBuilderProgramData.KindEnum.LIST)
                      .columnName("gender_identity"),
                  new SnapshotBuilderProgramData()
                      .id(4)
                      .name("Race")
                      .kind(SnapshotBuilderProgramData.KindEnum.LIST)
                      .columnName("race")))
          .featureValueGroups(
              List.of(
                  new SnapshotBuilderFeatureValueGroup()
                      .id(0)
                      .name("Condition")
                      .values(List.of("Condition Column 1", "Condition Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(1)
                      .name("Observation")
                      .values(List.of("Observation Column 1", "Observation Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(2)
                      .name("Procedure")
                      .values(List.of("Procedure Column 1", "Procedure Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(3)
                      .name("Surveys")
                      .values(List.of("Surveys Column 1", "Surveys Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .id(4)
                      .name("Person")
                      .values(List.of("Demographics Column 1", "Demographics Column 2"))))
          .prepackagedDatasetConceptSets(
              List.of(
                  new SnapshotBuilderDatasetConceptSets()
                      .name("Demographics")
                      .featureValueGroupName("Person"),
                  new SnapshotBuilderDatasetConceptSets()
                      .name("All surveys")
                      .featureValueGroupName("Surveys")));
}
