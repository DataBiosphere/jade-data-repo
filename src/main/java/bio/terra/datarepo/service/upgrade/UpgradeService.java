package bio.terra.datarepo.service.upgrade;

import bio.terra.datarepo.app.configuration.ApplicationConfiguration;
import bio.terra.datarepo.common.exception.NotImplementedException;
import bio.terra.datarepo.model.UpgradeModel;
import bio.terra.datarepo.service.iam.AuthenticatedUserRequest;
import bio.terra.datarepo.service.iam.IamAction;
import bio.terra.datarepo.service.iam.IamResourceType;
import bio.terra.datarepo.service.iam.IamService;
import bio.terra.datarepo.service.job.JobService;
import bio.terra.datarepo.service.upgrade.exception.InvalidCustomNameException;
import bio.terra.stairway.Flight;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpgradeService {

  private enum CustomFlight {
    PLACEHOLDER(null);

    private final Class<? extends Flight> flightClass;

    CustomFlight(Class<? extends Flight> flightClass) {
      this.flightClass = flightClass;
    }
  }

  private final JobService jobService;
  private final ApplicationConfiguration appConfig;
  private final IamService iamService;

  @Autowired
  public UpgradeService(
      JobService jobService, ApplicationConfiguration appConfig, IamService iamService) {
    this.jobService = jobService;
    this.appConfig = appConfig;
    this.iamService = iamService;
  }

  public String upgrade(UpgradeModel request, AuthenticatedUserRequest user) {
    // Make sure the user is a steward by checking for list jobs action
    iamService.verifyAuthorization(
        user, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.LIST_JOBS);

    if (request.getUpgradeType() != UpgradeModel.UpgradeTypeEnum.CUSTOM) {
      throw new NotImplementedException(
          "Upgrade type is not implemented: " + request.getUpgradeType().name());
    }

    // Note: in order to allow for an extensible endpoint, we do not require that customName is
    // filled in.
    // That means it could be null here. We could do a separate null check, but the valueOf also
    // does the
    // check and throws the NPE.
    final CustomFlight customFlight;
    try {
      customFlight = CustomFlight.valueOf(request.getCustomName());
    } catch (NullPointerException ex) {
      throw new InvalidCustomNameException("Custom name is required for custom upgrade type");
    } catch (IllegalArgumentException ex) {
      throw new InvalidCustomNameException(
          "Invalid custom name provided to upgrade: " + request.getCustomName());
    }

    return jobService
        .newJob(request.getCustomName(), customFlight.flightClass, request, user)
        .submit();
  }
}
