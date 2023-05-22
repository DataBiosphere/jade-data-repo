package bio.terra.service.admin.flight;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DrsAliasModel;
import bio.terra.service.filedata.DrsDao;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import org.springframework.context.ApplicationContext;

/** Flight use to ingest a DRS aliases into TDR */
public class DrsAliasRegisterFlight extends Flight {

  public DrsAliasRegisterFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // Initialization
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    List<DrsAliasModel> aliases =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), new TypeReference<>() {});

    DrsIdService drsIdService = appContext.getBean(DrsIdService.class);
    DrsDao drsDao = appContext.getBean(DrsDao.class);

    // Steps
    addStep(new DrsAliasVerifyStep(drsIdService, aliases, userReq));
    addStep(new DrsAliasRegisterStep(drsDao, drsIdService, aliases, userReq));
  }
}
