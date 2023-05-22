package bio.terra.service.admin.flight;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DrsAliasModel;
import bio.terra.service.filedata.DrsDao;
import bio.terra.service.filedata.DrsDao.DrsAliasSpec;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DrsAliasRegisterStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(DrsAliasRegisterStep.class);

  private static final int BATCH_SIZE = 5000;

  private final DrsDao drsDao;
  private final DrsIdService drsIdService;
  private final List<DrsAliasModel> aliases;
  private final AuthenticatedUserRequest userReq;

  public DrsAliasRegisterStep(
      DrsDao drsDao,
      DrsIdService drsIdService,
      List<DrsAliasModel> aliases,
      AuthenticatedUserRequest userReq) {
    this.drsDao = drsDao;
    this.drsIdService = drsIdService;
    this.aliases = aliases;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    List<DrsAliasSpec> aliasSpecs =
        aliases.stream()
            .map(
                a ->
                    new DrsAliasSpec(
                        a.getAliasDrsObjectId(), drsIdService.fromObjectId(a.getTdrDrsObjectId())))
            .toList();
    List<List<DrsAliasSpec>> batches = ListUtils.partition(aliasSpecs, BATCH_SIZE);
    int numBatches = batches.size();
    int batchNum = 0;
    for (var batch : batches) {
      logger.info("Ingesting batch {} of {} aliases", ++batchNum, numBatches);
      drsDao.insertDrsAlias(batch, context.getFlightId(), userReq);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Delete all aliases that were ingested by this flight
    drsDao.deleteDrsAliasByFlight(context.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
