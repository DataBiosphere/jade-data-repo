package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Integration;
import bio.terra.model.ConfigFaultCountedModel;
import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

// This test provides a method that performs a simple scenario of creating a dataset, ingesting some
// rows,
// making a snapshot, and then deleting everything.
//
// The tests that drive that method can configure faults to test underlying mechanisms.
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class SimpleScenarioFaultTests extends UsersBase {
  private final Logger logger = LoggerFactory.getLogger(SimpleScenarioFaultTests.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Rule @Autowired public TestJobWatcher testWatcher;

  private UUID profileId;
  private UUID datasetId;
  private UUID snapshotId;

  @Before
  public void setup() throws Exception {
    super.setup();
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);
  }

  // This is belts and suspenders, since we try to do these deletes in the scenario.
  // However, since we are testing faults, there might be failures...
  @After
  public void teardown() throws Exception {
    // Don't interrupt cleanup with the fault
    dataRepoFixtures.setFault(steward(), "SAM_TIMEOUT_FAULT", false);
    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshot(custodian(), snapshotId);
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDataset(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void testSamTimeout() throws Exception {
    ConfigGroupModel configGroup = buildConfigGroup(ConfigFaultCountedModel.RateStyleEnum.FIXED);
    List<ConfigModel> configList =
        dataRepoFixtures.setConfigList(steward(), configGroup).getItems();
    printConfigList("pre-fixed", configList);
    printConfigList("fixed", configGroup.getGroup());

    simpleScenario();

    // The rest of this is here not so much to test the fault as to validate the configuration test
    // infrastructure in a live environment.
    dataRepoFixtures.resetConfig(steward());
    configGroup = buildConfigGroup(ConfigFaultCountedModel.RateStyleEnum.RANDOM);
    dataRepoFixtures.setConfigList(steward(), configGroup);
    printConfigList("random", configGroup.getGroup());

    simpleScenario();

    configList = dataRepoFixtures.getConfigList(steward()).getItems();
    printConfigList("final", configList);
  }

  private ConfigGroupModel buildConfigGroup(ConfigFaultCountedModel.RateStyleEnum rateStyle) {
    ConfigGroupModel configGroupModel =
        new ConfigGroupModel().label("simpleScenarioFaultTests - SAM timeout fault - " + rateStyle);

    configGroupModel.addGroupItem(
        new ConfigModel()
            .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
            .name("SAM_RETRY_INITIAL_WAIT_SECONDS")
            .parameter(new ConfigParameterModel().value("1")));

    configGroupModel.addGroupItem(
        new ConfigModel()
            .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
            .name("SAM_RETRY_MAXIMUM_WAIT_SECONDS")
            .parameter(new ConfigParameterModel().value("3")));

    configGroupModel.addGroupItem(
        new ConfigModel()
            .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
            .name("SAM_OPERATION_TIMEOUT_SECONDS")
            .parameter(new ConfigParameterModel().value("300")));

    configGroupModel.addGroupItem(
        new ConfigModel()
            .configType(ConfigModel.ConfigTypeEnum.FAULT)
            .name("SAM_TIMEOUT_FAULT")
            .fault(
                new ConfigFaultModel()
                    .faultType(ConfigFaultModel.FaultTypeEnum.COUNTED)
                    .enabled(true)
                    .counted(
                        new ConfigFaultCountedModel()
                            .insert(-1)
                            .rate(20)
                            .rateStyle(rateStyle)
                            .skipFor(0))));

    return configGroupModel;
  }

  private void printConfigList(String label, List<ConfigModel> configModelList) {
    int index = 0;
    logger.info("Config model list - " + label);
    for (ConfigModel configModel : configModelList) {
      logger.info("Config model [" + index + "]: " + configModel);
      index++;
    }
  }

  private void simpleScenario() throws Exception {
    // TODO: Since add policy is sync, it doesn't survive the fault. So for now, turn it off
    //  for those operations.
    dataRepoFixtures.setFault(steward(), "SAM_TIMEOUT_FAULT", false);
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
    dataRepoFixtures.setFault(steward(), "SAM_TIMEOUT_FAULT", true);

    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

    ingestRequest = dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-file.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            custodian(), datasetSummaryModel.getName(), profileId, "ingest-test-snapshot.json");
    snapshotId = snapshotSummary.getId();

    // TODO: ditto from above
    dataRepoFixtures.setFault(steward(), "SAM_TIMEOUT_FAULT", false);
    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshot(custodian(), snapshotId);
      snapshotId = null;
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDataset(steward(), datasetId);
      datasetId = null;
    }
    dataRepoFixtures.setFault(steward(), "SAM_TIMEOUT_FAULT", true);
  }
}
