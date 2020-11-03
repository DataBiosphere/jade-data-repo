package bio.terra.service.configuration;

import bio.terra.model.RepositoryStatusModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.iam.IamProviderInterface;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

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

        if (configurationService.testInsertFault(ConfigEnum.LIVENESS_FAULT)) {
            logger.info("LIVENESS_FAULT insertion - failing status response");
            statusModel.setOk(false);
            return statusModel;
        }

        statusModel.putSystemsItem("Postgres", postgresStatus());
        statusModel.putSystemsItem("Sam", samStatus());

        // if critical system is down, then isOk = false
        statusModel.setOk(statusModel.getSystems().values().stream()
            .noneMatch(sys -> sys.isCritical() && !sys.isOk()));

        return statusModel;
    }

    private RepositoryStatusModelSystems postgresStatus() {
        // test by inserting fault
        if (configurationService.testInsertFault(ConfigEnum.CRITICAL_SYSTEM_FAULT)) {
            logger.info("CRITICAL_SYSTEM_FAULT inserted for test - setting postgres system status to failing");
            return new RepositoryStatusModelSystems()
                .ok(false)
                .critical(true)
                .message("CRITICAL_SYSTEM_FAULT inserted for test");
        }

        Boolean dbStatus = false;
        String Msg;
        try {
            datasetDao.probeDatabase();
            dbStatus = true;
            Msg = "Successfully queried database.";
        } catch (DataAccessException ex) {
            Msg = "Failed to query database: " + ex.getMessage();
            logger.error("Failed to query database. Ex: {}", ex);
        }
        RepositoryStatusModelSystems databaseSystem = new RepositoryStatusModelSystems()
            .ok(dbStatus)
            .critical(true)
            .message(Msg);
        return databaseSystem;
    }

    private RepositoryStatusModelSystems samStatus() {
        Boolean samStatus = false;
        String Msg;
        try {
            SystemStatus samStatusModel = iamProviderInterface.samStatus();
            samStatus = samStatusModel.getOk();
            Msg = samStatusModel.getSystems().toString();
        } catch (Exception ex) {
            logger.error("Failed to complete sam status check. Ex: {}", ex);
            Msg = ex.getMessage();
        }
        RepositoryStatusModelSystems samSystem = new RepositoryStatusModelSystems()
            .ok(samStatus)
            .critical(true)
            .message(Msg);
        return samSystem;
    }
}
