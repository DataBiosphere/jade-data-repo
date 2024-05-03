package bio.terra.service.admin.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.model.DrsAliasModel;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DrsAliasVerifyStepTest {

  DrsIdService drsIdService;

  @BeforeEach
  void setUp() {
    ApplicationConfiguration appConfig = new ApplicationConfiguration();
    appConfig.setDnsName("foo.org");
    drsIdService = new DrsIdService(appConfig);
  }

  @Test
  void testHappyPath() throws InterruptedException {
    UUID fileId = UUID.randomUUID();
    testDrsVerification(
        List.of(
            new DrsAliasModel().aliasDrsObjectId("foo").tdrDrsObjectId("v2_" + fileId),
            new DrsAliasModel().aliasDrsObjectId("bar").tdrDrsObjectId("v2_" + fileId)),
        StepResult.getStepResultSuccess());
  }

  @Test
  void testInvalidTdrIdFails() throws InterruptedException {
    UUID fileId = UUID.randomUUID();
    testDrsVerification(
        List.of(
            new DrsAliasModel().aliasDrsObjectId("foo").tdrDrsObjectId("baddrsid"),
            new DrsAliasModel().aliasDrsObjectId("bar").tdrDrsObjectId("v2_" + fileId)),
        new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            new IllegalArgumentException("One or more TDR Ids are invalid")));
  }

  @Test
  void testInvalidAliasIdFails() throws InterruptedException {
    UUID fileId = UUID.randomUUID();
    testDrsVerification(
        List.of(
            new DrsAliasModel().aliasDrsObjectId("v2_" + fileId).tdrDrsObjectId("v2_" + fileId),
            new DrsAliasModel().aliasDrsObjectId("bar").tdrDrsObjectId("v2_" + fileId)),
        new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            new IllegalArgumentException(
                "One or more aliases are valid TDR DRS IDs which is not allowed")));
  }

  private void testDrsVerification(List<DrsAliasModel> aliases, StepResult expectedResult)
      throws InterruptedException {
    DrsAliasVerifyStep step = new DrsAliasVerifyStep(drsIdService, aliases);
    // You unfortunately can't compare step objects so we are left to do a string comparison
    assertThat(step.doStep(null).toString(), equalTo(expectedResult.toString()));
  }
}
