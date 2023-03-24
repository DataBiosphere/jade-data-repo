package bio.terra.service.cohortbuilder;

import bio.terra.model.Cohort;
import bio.terra.model.CohortCreateInfo;
import bio.terra.model.CohortList;
import bio.terra.model.CohortUpdateInfo;
import bio.terra.model.Criteria;
import bio.terra.model.CriteriaGroup;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class CohortService {

  public CohortList listCohorts(Integer offset, Integer limit) {
    CohortList cohortList = new CohortList();
    cohortList.add(generateStubCohort());
    return cohortList;
  }

  public Cohort createCohort(CohortCreateInfo cohortCreateInfo) {
    return generateStubCohort();
  }

  public Cohort getCohort(String cohortId) {
    return generateStubCohort();
  }

  public Cohort updateCohort(String cohortId, CohortUpdateInfo cohortUpdateInfo) {
    return generateStubCohort();
  }

  public void deleteCohort(String cohortId) {}

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
                                    "{\"type\":\"classification\",\"id\":\"bio.terra.tanagra-conditions\",\"title\":\"Condition\",\"conceptSet\":true,\"category\":\"Domains\",\"columns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Concept name\"},{\"key\":\"id\",\"width\":100,\"title\":\"Concept ID\"},{\"key\":\"standard_concept\",\"width\":120,\"title\":\"Source/standard\"},{\"key\":\"vocabulary_t_value\",\"width\":120,\"title\":\"Vocab\"},{\"key\":\"concept_code\",\"width\":120,\"title\":\"Code\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"hierarchyColumns\":[{\"key\":\"name\",\"width\":\"100%\",\"title\":\"Condition\"},{\"key\":\"id\",\"width\":120,\"title\":\"Concept ID\"},{\"key\":\"t_rollup_count\",\"width\":120,\"title\":\"Roll-up count\"}],\"occurrence\":\"condition_occurrence\",\"classification\":\"condition\"}")))));
  }
}
