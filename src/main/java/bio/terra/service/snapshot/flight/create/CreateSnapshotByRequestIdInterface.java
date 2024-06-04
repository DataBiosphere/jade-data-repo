package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.*;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface CreateSnapshotByRequestIdInterface {
  default StepResult prepareAndCreateSnapshot(
      FlightContext context,
      Snapshot snapshot,
      SnapshotRequestModel snapshotReq,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotRequestDao snapshotRequestDao,
      SnapshotDao snapshotDao,
      AuthenticatedUserRequest userReq)
      throws InterruptedException {
    UUID accessRequestId =
        snapshotReq.getContents().get(0).getRequestIdSpec().getSnapshotRequestId();
    SnapshotAccessRequestResponse accessRequest = snapshotRequestDao.getById(accessRequestId);

    UUID sourceSnapshotId = accessRequest.getSourceSnapshotId();
    Snapshot sourceSnapshot = snapshotDao.retrieveSnapshot(sourceSnapshotId);
    Dataset dataset = sourceSnapshot.getSourceDataset();
    AssetSpecification assetSpecification =
        buildAssetFromSnapshotAccessRequest(dataset, accessRequest);
    String sqlQuery =
        snapshotBuilderService.generateRowIdQuery(accessRequest, sourceSnapshot, userReq);
    Instant createdAt = sourceSnapshot.getCreatedDate();
    return createSnapshot(context, assetSpecification, snapshot, sqlQuery, createdAt);
  }

  StepResult createSnapshot(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException;

  static AssetSpecification buildAssetFromSnapshotAccessRequest(
      Dataset dataset, SnapshotAccessRequestResponse snapshotRequestModel) {
    // build asset model from snapshot request
    AssetModel assetModel = new AssetModel();
    SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyyyyHHmmss");
    String uniqueTimestamp = dateFormat.format(new Date());
    assetModel.name("snapshot-by-request-asset-" + uniqueTimestamp);
    // TODO - instead pull root table, root column and follow from snapshot builder settings
    assetModel.rootTable("person");
    assetModel.rootColumn("person_id");
    // Manually add dictionary tables, leave columns empty to return all columns
    assetModel.addTablesItem(new AssetTableModel().name("person"));
    assetModel.addTablesItem(new AssetTableModel().name("concept"));

    // Build asset tables and columns based on the concept sets included in the snapshot request
    List<SnapshotBuilderTable> tables = pullTables(snapshotRequestModel);
    // First pass - just add tables and follow, an empty set of columns will return all columns
    tables.forEach(
        table -> {
          assetModel.addTablesItem(new AssetTableModel().name(table.getDatasetTableName()));
          assetModel.follow(table.getRelationships());
        });

    // Make sure we just built a valid asset model
    dataset.validateDatasetAssetSpecification(assetModel);

    // convert the asset model to an asset specification
    return dataset.getNewAssetSpec(assetModel);
  }

  static List<SnapshotBuilderTable> pullTables(SnapshotAccessRequestResponse snapshotRequestModel) {
    var valueSets = snapshotRequestModel.getSnapshotSpecification().getValueSets();
    var valueSetNames = valueSets.stream().map(SnapshotBuilderFeatureValueGroup::getName).toList();

    Map<String, SnapshotBuilderTable> tableMap = populateManualTableMap();

    return valueSetNames.stream()
        .map(
            name -> {
              if (tableMap.containsKey(name)) {
                return tableMap.get(name);
              } else {
                throw new IllegalArgumentException("Unknown value set name: " + name);
              }
            })
        .toList();
  }

  private static Map<String, SnapshotBuilderTable> populateManualTableMap() {
    // manual definition of domain names -> dataset table
    Map<String, SnapshotBuilderTable> tableMap = new HashMap<>();
    tableMap.put("Demographics", new SnapshotBuilderTable().datasetTableName("person"));
    tableMap.put(
        "Drug",
        new SnapshotBuilderTable()
            .datasetTableName("drug_exposure")
            .relationships(
                List.of(
                    "fpk_drug_person",
                    "fpk_drug_type_concept",
                    "fpk_drug_concept",
                    "fpk_drug_route_concept",
                    "fpk_drug_concept_s")));
    tableMap.put(
        "Measurement",
        new SnapshotBuilderTable()
            .datasetTableName("measurement")
            .relationships(
                List.of(
                    "fpk_measurement_person",
                    "fpk_measurement_concept",
                    "fpk_measurement_unit",
                    "fpk_measurement_concept_s",
                    "fpk_measurement_value",
                    "fpk_measurement_type_concept",
                    "fpk_measurement_operator")));
    tableMap.put(
        "Visit",
        new SnapshotBuilderTable()
            .datasetTableName("visit_occurrence")
            .relationships(
                List.of(
                    "fpk_visit_person",
                    "fpk_visit_preceding",
                    "fpk_visit_concept_s",
                    "fpk_visit_type_concept",
                    "fpk_visit_concept",
                    "fpk_visit_discharge")));
    tableMap.put(
        "Device",
        new SnapshotBuilderTable()
            .datasetTableName("device_exposure")
            .relationships(
                List.of(
                    "fpk_device_person",
                    "fpk_device_concept",
                    "fpk_device_concept_s",
                    "fpk_device_type_concept")));
    tableMap.put(
        "Condition",
        new SnapshotBuilderTable()
            .datasetTableName("condition_occurrence")
            .relationships(
                List.of(
                    "fpk_condition_person",
                    "fpk_condition_concept",
                    "fpk_condition_status_concept",
                    "fpk_condition_concept_s")));
    tableMap.put(
        "Procedure",
        new SnapshotBuilderTable()
            .datasetTableName("procedure_occurrence")
            .relationships(
                List.of(
                    "fpk_procedure_person",
                    "fpk_procedure_concept",
                    "fpk_procedure_concept_s",
                    "fpk_procedure_type_concept",
                    "fpk_procedure_modifier")));
    tableMap.put(
        "Observation",
        new SnapshotBuilderTable()
            .datasetTableName("observation")
            .relationships(
                List.of(
                    "fpk_observation_person",
                    "fpk_observation_period_person",
                    "fpk_observation_concept",
                    "fpk_observation_concept_s",
                    "fpk_observation_unit",
                    "fpk_observation_qualifier",
                    "fpk_observation_type_concept",
                    "fpk_observation_period_concept",
                    "fpk_observation_value")));
    tableMap.put("Year of Birth", new SnapshotBuilderTable().datasetTableName("person"));
    tableMap.put(
        "Ethnicity",
        new SnapshotBuilderTable()
            .datasetTableName("person")
            .relationships(
                List.of("fpk_person_ethnicity_concept", "fpk_person_ethnicity_concept_s")));
    tableMap.put(
        "Race",
        new SnapshotBuilderTable()
            .datasetTableName("person")
            .relationships(List.of("fpk_person_race_concept")));

    // Add secondary relationships?
    // drug_exposure ========================
    // drug_exposure & visit_occurrence = fpk_drug_visit
    // drug_exposure & visit_detail = fpk_drug_v_detail
    // measurement ========================
    // measurement & visit_detail = fpk_measurement_v_detail
    // measurement & visit_occurrence = fpk_measurement_visit
    // measurement & provider = fpk_measurement_provider
    // measurement & visit_detail = fpk_measurement_v_detail
    // visit_occurrence ========================
    // visit_occurrence & note = fpk_note_visit
    // visit_occurrence & device_exposure = fpk_device_visit
    // visit_occurrence & survey_conduct = fpk_survey_visit, fpk_response_visit
    // visit_occurrence & procedure_occurrence = fpk_procedure_visit
    // visit_occurrence & provider = fpk_visit_provider
    // visit_occurrence & care_site = fpk_visit_care_site
    // visit_occurrence & condition_occurrence = fpk_condition_visit
    // visit_occurrence & visit_detail = fpd_v_detail_visit
    // visit_occurrence & observation = fpk_observation_visit
    // device_exposure ======================
    // device_exposure & provider = fpk_device_provider
    // device_exposure & visit_detail = fpk_device_v_detail
    // device_exposure & visit_occurrence = fpk_device_visit
    // condition_occurrence =================
    // condition_occurrence & visit_detail = fpk_condition_v_detail
    // condition_occurrence & visit_occurrence = fpk_condition_visit
    // condition_occurrence & provider = fpk_condition_provider
    // procedure_occurrence =================
    // procedure_occurrence & visit_detail = fpk_procedure_v_detail
    // procedure_occurrence & visit_occurrence = fpk_procedure_visit
    // procedure_occurrence & provider = fpk_procedure_provider
    // observation ==========================
    // observation & visit_detail = fpk_observation_v_detail
    // observation & visit_occurrence = fpk_observation_visit
    // observation & provider = fpk_observation_provider

    return tableMap;
  }
}
