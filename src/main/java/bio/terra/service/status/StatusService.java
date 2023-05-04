package bio.terra.service.status;

import bio.terra.model.RepositoryStatusModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.duos.DuosService;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.resourcemanagement.BufferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StatusService {

  private static final Logger logger = LoggerFactory.getLogger(StatusService.class);
  private final ConfigurationService configurationService;
  private final DatasetDao datasetDao;
  private final IamProviderInterface iamProviderInterface;
  private final BufferService bufferService;
  private final DuosService duosService;
  private final PolicyService policyService;

  // Names of subservices included in status check
  public static final String POSTGRES = "Postgres";
  public static final String SAM = "Sam";
  public static final String RBS = "ResourceBufferService";
  public static final String DUOS = "DUOS";
  public static final String TPS = "TerraPolicyService";

  @Autowired
  public StatusService(
      ConfigurationService configurationService,
      DatasetDao datasetDao,
      IamProviderInterface iamProviderInterface,
      BufferService bufferService,
      DuosService duosService,
      PolicyService policyService) {
    this.configurationService = configurationService;
    this.datasetDao = datasetDao;
    this.iamProviderInterface = iamProviderInterface;
    this.bufferService = bufferService;
    this.duosService = duosService;
    this.policyService = policyService;
  }

  public RepositoryStatusModel getStatus() {
    RepositoryStatusModel statusModel = new RepositoryStatusModel();

    // Used by Unit test: StatusTest
    if (configurationService.testInsertFault(ConfigEnum.LIVENESS_FAULT)) {
      logger.info("LIVENESS_FAULT insertion - failing status response");
      statusModel.setOk(false);
      return statusModel;
    }

    statusModel.putSystemsItem(POSTGRES, datasetDao.statusCheck().critical(true));
    statusModel.putSystemsItem(SAM, iamProviderInterface.samStatus().critical(true));
    statusModel.putSystemsItem(RBS, bufferService.status());
    statusModel.putSystemsItem(DUOS, duosService.status());
    statusModel.putSystemsItem(TPS, policyService.status());

    // if all critical systems are ok, then isOk = true
    // if any one critical system is down, then isOk = false
    statusModel.setOk(
        statusModel.getSystems().values().stream()
            .noneMatch(sys -> sys.isCritical() && !sys.isOk()));

    return statusModel;
  }
}
