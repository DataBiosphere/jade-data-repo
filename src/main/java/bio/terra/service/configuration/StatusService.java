package bio.terra.service.configuration;

import bio.terra.model.RepositoryStatusModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.iam.IamProviderInterface;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.util.concurrent.Callable;

@Service
public class StatusService {

    private final Logger logger = LoggerFactory.getLogger(StatusService.class);
    private final ConfigurationService configurationService;
    private final DatasetDao datasetDao;
    private final IamProviderInterface iamProviderInterface;

    @Autowired
    public StatusService(
        ConfigurationService configurationService,
        DatasetDao datasetDao,
        IamProviderInterface iamProviderInterface) {
        this.configurationService = configurationService;
        this.datasetDao = datasetDao;
        this.iamProviderInterface = iamProviderInterface;
    }

    public RepositoryStatusModel getStatus() {
        RepositoryStatusModel statusModel = new RepositoryStatusModel();

        // Used by Unit test: StatusTest
        if (configurationService.testInsertFault(ConfigEnum.LIVENESS_FAULT)) {
            logger.info("LIVENESS_FAULT insertion - failing status response");
            statusModel.setOk(false);
            return statusModel;
        }

        RepositoryStatusModelSystems postgresStatus =
            checkStatus("postgresStatus", this::postgresStatusInner, true);
        statusModel.putSystemsItem("Postgres", postgresStatus);
        RepositoryStatusModelSystems samStatus =
            checkStatus("samStatus", this::samStatusInner, true);
        statusModel.putSystemsItem("Sam", samStatus);

        // if all critical systems are ok, then isOk = true
        // if any one critical system is down, then isOk = false
        statusModel.setOk(statusModel.getSystems().values().stream()
            .noneMatch(sys -> sys.isCritical() && !sys.isOk()));

        return statusModel;
    }

    private RepositoryStatusModelSystems checkStatus(String statusFnName, Callable<RepositoryStatusModelSystems> statusFn, boolean isCritical) {
        RepositoryStatusModelSystems statusModel;
        try {
            statusModel = statusFn.call();
            statusModel.setCritical(isCritical);
        } catch (Exception ex) {
            logger.error("Error during {}, exception was: {}", statusFnName, ex);
            statusModel = new RepositoryStatusModelSystems()
                .ok(false)
                .critical(isCritical)
                .message(ex.getMessage());
        }
        return statusModel;
    }

    // Use in conjunction with checkStatus() to get postgres status
    private RepositoryStatusModelSystems postgresStatusInner() throws DataAccessException {
        // Used by Unit test: StatusTest
        if (configurationService.testInsertFault(ConfigEnum.CRITICAL_SYSTEM_FAULT)) {
            logger.info("CRITICAL_SYSTEM_FAULT inserted for test - setting postgres system status to failing");
            return new RepositoryStatusModelSystems()
                .ok(false)
                .critical(true)
                .message("CRITICAL_SYSTEM_FAULT inserted for test");
        }

        // if can successfully complete call without throwing exception, then system is up
        datasetDao.probeDatabase();
        RepositoryStatusModelSystems databaseSystem = new RepositoryStatusModelSystems()
            .ok(true)
            .message("Successfully queried database.");
        return databaseSystem;
    }
    
    private RepositoryStatusModelSystems samStatusInner() throws ApiException {
        SystemStatus samStatusModel = iamProviderInterface.samStatus();

        RepositoryStatusModelSystems samSystem = new RepositoryStatusModelSystems()
            .ok(samStatusModel.getOk())
            .message(samStatusModel.getSystems().toString());
        return samSystem;
    }
}
