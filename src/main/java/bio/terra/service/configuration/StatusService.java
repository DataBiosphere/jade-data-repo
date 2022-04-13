package bio.terra.service.configuration;

import bio.terra.model.RepositoryStatusModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.resourcemanagement.BufferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

@Service
public class StatusService {

  private static final Logger logger = LoggerFactory.getLogger(StatusService.class);
  private final ConfigurationService configurationService;
  private final DatasetDao datasetDao;
  private final IamProviderInterface iamProviderInterface;
  private final BufferService bufferService;

  @Autowired
  public StatusService(
      ConfigurationService configurationService,
      DatasetDao datasetDao,
      IamProviderInterface iamProviderInterface,
      BufferService bufferService) {
    this.configurationService = configurationService;
    this.datasetDao = datasetDao;
    this.iamProviderInterface = iamProviderInterface;
    this.bufferService = bufferService;
  }

  public RepositoryStatusModel getStatus() {
    RepositoryStatusModel statusModel = new RepositoryStatusModel();

    // Used by Unit test: StatusTest
    if (configurationService.testInsertFault(ConfigEnum.LIVENESS_FAULT)) {
      logger.info("LIVENESS_FAULT insertion - failing status response");
      statusModel.setOk(false);
      return statusModel;
    }

    statusModel.putSystemsItem("Postgres", postgresStatus(true));
    statusModel.putSystemsItem("Sam", iamProviderInterface.samStatus().critical(true));
    statusModel.putSystemsItem("ResourceBufferService", bufferService.status());

    // if all critical systems are ok, then isOk = true
    // if any one critical system is down, then isOk = false
    statusModel.setOk(
        statusModel.getSystems().values().stream()
            .noneMatch(sys -> sys.isCritical() && !sys.isOk()));

    return statusModel;
  }

  private RepositoryStatusModelSystems postgresStatus(Boolean isCritical)
      throws DataAccessException {
    // Used by Unit test: StatusTest
    if (configurationService.testInsertFault(ConfigEnum.CRITICAL_SYSTEM_FAULT)) {
      logger.info(
          "CRITICAL_SYSTEM_FAULT inserted for test - setting postgres system status to failing");
      return new RepositoryStatusModelSystems()
          .ok(false)
          .critical(true)
          .message("CRITICAL_SYSTEM_FAULT inserted for test");
    }

    return datasetDao.statusCheck().critical(isCritical);
  }
}
