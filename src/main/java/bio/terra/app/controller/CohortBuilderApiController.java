package bio.terra.app.controller;

import bio.terra.controller.CohortBuilderApi;
import bio.terra.model.Cohort;
import bio.terra.model.CohortCreateInfo;
import bio.terra.model.CohortList;
import bio.terra.model.CohortUpdateInfo;
import bio.terra.model.ConceptSet;
import bio.terra.model.ConceptSetCreateInfo;
import bio.terra.model.ConceptSetList;
import bio.terra.model.ConceptSetUpdateInfo;
import bio.terra.model.CountQuery;
import bio.terra.model.DisplayHintList;
import bio.terra.model.HintQuery;
import bio.terra.model.InstanceCountList;
import bio.terra.model.InstanceList;
import bio.terra.model.Query;
import bio.terra.service.cohortbuilder.CohortService;
import bio.terra.service.cohortbuilder.ConceptSetService;
import bio.terra.service.cohortbuilder.HintsService;
import bio.terra.service.cohortbuilder.InstancesService;
import io.swagger.annotations.Api;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@Api(tags = {"cohort-builder"})
public class CohortBuilderApiController implements CohortBuilderApi {
  private final CohortService cohortService;
  private final ConceptSetService conceptSetService;
  private final HintsService hintsService;
  private final InstancesService instancesService;

  public CohortBuilderApiController(
      CohortService cohortService,
      ConceptSetService conceptSetService,
      HintsService hintsService,
      InstancesService instancesService) {
    this.cohortService = cohortService;
    this.conceptSetService = conceptSetService;
    this.hintsService = hintsService;
    this.instancesService = instancesService;
  }

  @Override
  public ResponseEntity<CohortList> listCohorts(Integer offset, Integer limit) {
    return ResponseEntity.ok(cohortService.listCohorts(offset, limit));
  }

  @Override
  public ResponseEntity<Cohort> createCohort(CohortCreateInfo cohortCreateInfo) {
    return ResponseEntity.ok(cohortService.createCohort(cohortCreateInfo));
  }

  @Override
  public ResponseEntity<Cohort> getCohort(String cohortId) {
    return ResponseEntity.ok(cohortService.getCohort(cohortId));
  }

  @Override
  public ResponseEntity<Cohort> updateCohort(String cohortId, CohortUpdateInfo cohortUpdateInfo) {
    return ResponseEntity.ok(cohortService.updateCohort(cohortId, cohortUpdateInfo));
  }

  @Override
  public ResponseEntity<Void> deleteCohort(String cohortId) {
    cohortService.deleteCohort(cohortId);
    return ResponseEntity.noContent().build();
  }

  @Override
  public ResponseEntity<InstanceCountList> countInstances(
      String entityName, CountQuery countQuery) {
    return ResponseEntity.ok(instancesService.countInstances(entityName, countQuery));
  }

  @Override
  public ResponseEntity<InstanceList> queryInstances(String entityName, Query query) {
    return ResponseEntity.ok(instancesService.queryInstances(entityName, query));
  }

  @Override
  public ResponseEntity<ConceptSetList> listConceptSets(Integer offset, Integer limit) {
    return ResponseEntity.ok(conceptSetService.listConceptSets(offset, limit));
  }

  @Override
  public ResponseEntity<ConceptSet> createConceptSet(ConceptSetCreateInfo conceptSetCreateInfo) {
    return ResponseEntity.ok(conceptSetService.createConceptSet(conceptSetCreateInfo));
  }

  @Override
  public ResponseEntity<ConceptSet> getConceptSet(String conceptSetId) {
    return ResponseEntity.ok(conceptSetService.getConceptSet(conceptSetId));
  }

  @Override
  public ResponseEntity<ConceptSet> updateConceptSet(
      String conceptSetId, ConceptSetUpdateInfo conceptSetUpdateInfo) {
    return ResponseEntity.ok(
        conceptSetService.updateConceptSet(conceptSetId, conceptSetUpdateInfo));
  }

  @Override
  public ResponseEntity<Void> deleteConceptSet(String conceptSetId) {
    conceptSetService.deleteConceptSet(conceptSetId);
    return ResponseEntity.noContent().build();
  }

  @Override
  public ResponseEntity<DisplayHintList> queryHints(String entityId, HintQuery hintQuery) {
    return ResponseEntity.ok(hintsService.queryHints(entityId, hintQuery));
  }
}
