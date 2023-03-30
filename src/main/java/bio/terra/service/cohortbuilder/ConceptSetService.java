package bio.terra.service.cohortbuilder;

import bio.terra.model.ConceptSet;
import bio.terra.model.ConceptSetCreateInfo;
import bio.terra.model.ConceptSetList;
import bio.terra.model.ConceptSetUpdateInfo;
import bio.terra.model.Criteria;
import java.time.OffsetDateTime;
import org.springframework.stereotype.Component;

@Component
public class ConceptSetService {

  public ConceptSetList listConceptSets(Integer offset, Integer limit) {
    ConceptSetList conceptSetList = new ConceptSetList();
    conceptSetList.add(generateStubConceptSet());
    return conceptSetList;
  }

  public ConceptSet createConceptSet(ConceptSetCreateInfo conceptSetCreateInfo) {
    return generateStubConceptSet();
  }

  public ConceptSet getConceptSet(String ConceptSetId) {
    return generateStubConceptSet();
  }

  public ConceptSet updateConceptSet(
      String conceptSetId, ConceptSetUpdateInfo conceptSetUpdateInfo) {
    return generateStubConceptSet();
  }

  public void deleteConceptSet(String conceptSetId) {}

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
