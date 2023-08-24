package bio.terra.service.snapshotbuilder;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderFeatureValueGroup;
import bio.terra.model.SnapshotBuilderListOption;
import bio.terra.model.SnapshotBuilderPrepackagedConceptSets;
import bio.terra.model.SnapshotBuilderProgramDataOption;
import bio.terra.model.SnapshotBuilderSettings;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  public SnapshotBuilderService() {}
  /**
   * Fetch the snapshot builder settings for a given dataset Currently just returns dummy data for
   * the sake of parallelizing UI and API development Will eventually be adapted to read from the DB
   *
   * @param datasetId in UUID format
   * @return a SnapshotBuilderSettings = API output-friendly representation of the Dataset's
   *     snapshot builder settings
   */
  public SnapshotBuilderSettings getSnapshotBuilderSettings(UUID datasetId) {
    return new SnapshotBuilderSettings()
        .domainOptions(
            List.of(
                new SnapshotBuilderDomainOption()
                    .id(10)
                    .category("Condition")
                    .conceptCount(18000)
                    .participantCount(12500)
                    .root(
                        new SnapshotBuilderConcept()
                            .id(100)
                            .name("Condition")
                            .count(100)
                            .hasChildren(true)),
                new SnapshotBuilderDomainOption()
                    .id(11)
                    .category("Procedure")
                    .conceptCount(22500)
                    .participantCount(11328)
                    .root(
                        new SnapshotBuilderConcept()
                            .id(200)
                            .name("Procedure")
                            .count(100)
                            .hasChildren(true)),
                new SnapshotBuilderDomainOption()
                    .id(12)
                    .category("Observation")
                    .conceptCount(12300)
                    .participantCount(23223)
                    .root(
                        new SnapshotBuilderConcept()
                            .id(300)
                            .name("Observation")
                            .count(100)
                            .hasChildren(true))))
        .programDataOptions(
            List.of(
                new SnapshotBuilderProgramDataOption()
                    .id(1)
                    .name("Year of birth")
                    .kind(SnapshotBuilderProgramDataOption.KindEnum.RANGE)
                    .min(1900)
                    .max(2023),
                new SnapshotBuilderProgramDataOption()
                    .id(2)
                    .name("Ethnicity")
                    .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                    .values(
                        List.of(
                            new SnapshotBuilderListOption().name("Hispanic or Latino").id(20),
                            new SnapshotBuilderListOption().name("Not Hispanic or Latino").id(21),
                            new SnapshotBuilderListOption().name("No Matching Concept").id(0))),
                new SnapshotBuilderProgramDataOption()
                    .id(3)
                    .name("Gender identity")
                    .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                    .values(
                        List.of(
                            new SnapshotBuilderListOption().name("FEMALE").id(22),
                            new SnapshotBuilderListOption().name("MALE").id(23),
                            new SnapshotBuilderListOption().name("NON BINARY").id(24),
                            new SnapshotBuilderListOption().name("GENDERQUEER").id(25),
                            new SnapshotBuilderListOption().name("TWO SPIRIT").id(26),
                            new SnapshotBuilderListOption().name("AGENDER").id(27),
                            new SnapshotBuilderListOption().name("No Matching Concept").id(0))),
                new SnapshotBuilderProgramDataOption()
                    .id(4)
                    .name("Race")
                    .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                    .values(
                        List.of(
                            new SnapshotBuilderListOption()
                                .name("American Indian or Alaska Native")
                                .id(28),
                            new SnapshotBuilderListOption().name("Asian").id(29),
                            new SnapshotBuilderListOption().name("Black").id(30),
                            new SnapshotBuilderListOption().name("White").id(31)))))
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
        .prepackagedConceptSets(
            List.of(
                new SnapshotBuilderPrepackagedConceptSets()
                    .name("Demographics")
                    .featureValueGroupName("Person"),
                new SnapshotBuilderPrepackagedConceptSets()
                    .name("All surveys")
                    .featureValueGroupName("Surveys")));
  }
}
