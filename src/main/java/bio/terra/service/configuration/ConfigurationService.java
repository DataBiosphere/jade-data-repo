package bio.terra.service.configuration;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.model.ConfigFaultCountedModel;
import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigListModel;
import bio.terra.model.ConfigModel;
import bio.terra.service.configuration.exception.ConfigNotFoundException;
import bio.terra.service.configuration.exception.DuplicateConfigNameException;
import bio.terra.service.iam.sam.SamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static bio.terra.service.configuration.ConfigEnum.BUCKET_LOCK_CONFLICT_CONTINUE_FAULT;
import static bio.terra.service.configuration.ConfigEnum.BUCKET_LOCK_CONFLICT_STOP_FAULT;
import static bio.terra.service.configuration.ConfigEnum.CREATE_ASSET_FAULT;
import static bio.terra.service.configuration.ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT;
import static bio.terra.service.configuration.ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT;
import static bio.terra.service.configuration.ConfigEnum.DATASET_GRANT_ACCESS_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_DELETE_LOCK_CONFLICT_CONTINUE_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_DELETE_LOCK_CONFLICT_STOP_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_INGEST_LOCK_CONFLICT_STOP_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_INGEST_LOCK_FATAL_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_INGEST_LOCK_RETRY_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_INGEST_UNLOCK_FATAL_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FILE_INGEST_UNLOCK_RETRY_FAULT;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_QUERY_BATCH_SIZE;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_VALIDATE_BATCH_SIZE;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_BATCH_SIZE;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_CACHE_SIZE;
import static bio.terra.service.configuration.ConfigEnum.LOAD_BULK_ARRAY_FILES_MAX;
import static bio.terra.service.configuration.ConfigEnum.LOAD_BULK_FILES_MAX;
import static bio.terra.service.configuration.ConfigEnum.LOAD_CONCURRENT_FILES;
import static bio.terra.service.configuration.ConfigEnum.LOAD_CONCURRENT_INGESTS;
import static bio.terra.service.configuration.ConfigEnum.LOAD_DRIVER_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE;
import static bio.terra.service.configuration.ConfigEnum.LOAD_HISTORY_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.LOAD_SKIP_FILE_LOAD;
import static bio.terra.service.configuration.ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_TIMEOUT_FAULT;
import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT;
import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT;
import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_GRANT_ACCESS_FAULT;
import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_GRANT_FILE_ACCESS_FAULT;
import static bio.terra.service.configuration.ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT;
import static bio.terra.service.configuration.ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT;
import static bio.terra.service.configuration.ConfigEnum.TABLE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT;
import static bio.terra.service.configuration.ConfigEnum.TABLE_INGEST_LOCK_CONFLICT_STOP_FAULT;


@Component
public class ConfigurationService {
    private final Logger logger = LoggerFactory.getLogger(ConfigurationService.class);

    private final ApplicationConfiguration appConfiguration;
    private final SamConfiguration samConfiguration;

    private Map<ConfigEnum, ConfigBase> configuration = new HashMap<>();

    @Autowired
    public ConfigurationService(SamConfiguration samConfiguration,
                                ApplicationConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
        this.samConfiguration = samConfiguration;
        setConfiguration();
    }

    // -- repository API methods --

    public ConfigListModel setConfig(ConfigGroupModel groupModel) {
        logger.info("Setting configuration - label: " + groupModel.getLabel());

        // Validate before setting any values
        for (ConfigModel configModel : groupModel.getGroup()) {
            ConfigBase config = lookup(configModel.getName());
            config.validate(configModel);
        }

        List<ConfigModel> priorConfigList = new LinkedList<>();
        for (ConfigModel configModel : groupModel.getGroup()) {
            ConfigEnum configEnum = ConfigEnum.lookupByApiName(configModel.getName());
            ConfigBase config = configuration.get(configEnum);
            ConfigModel prior = config.set(configModel);
            priorConfigList.add(prior);
        }

        return new ConfigListModel().items(priorConfigList).total(priorConfigList.size());
    }

    public ConfigModel getConfig(String name) {
        ConfigBase config = lookup(name);
        return config.get();
    }

    public ConfigListModel getConfigList() {
        List<ConfigModel> configList = new LinkedList<>();
        for (ConfigBase config : configuration.values()) {
            configList.add(config.get());
        }
        return new ConfigListModel().items(configList).total(configList.size());
    }

    public void reset() {
        for (ConfigBase config : configuration.values()) {
            config.reset();
        }
    }

    public void setFault(String name, boolean enable) {
        ConfigBase configBase = lookup(name);
        configBase.validateType(ConfigModel.ConfigTypeEnum.FAULT);

        ConfigFault fault = (ConfigFault) configBase;
        fault.setEnabled(enable);
        logger.info("Set fault " + name + " to " + enable);
    }

    // Exposed for use in unit test
    <T> void addParameter(ConfigEnum configEnum, T value) {
        checkDuplicate(configEnum);
        ConfigParameter param = new ConfigParameter(configEnum, value);
        configuration.put(configEnum, param);
    }

    public <T> T getParameterValue(ConfigEnum configEnum) {
        return lookupByEnum(configEnum).getCurrentValue();
    }

    void addFaultCounted(ConfigEnum configEnum,
                         int skipFor,
                         int insert,
                         int rate,
                         ConfigFaultCountedModel.RateStyleEnum rateStyle) {
        checkDuplicate(configEnum);
        ConfigFaultCountedModel countedModel = new ConfigFaultCountedModel()
            .skipFor(skipFor)
            .insert(insert)
            .rate(rate)
            .rateStyle(rateStyle);
        ConfigFaultCounted fault = new ConfigFaultCounted(configEnum,
            ConfigFaultModel.FaultTypeEnum.COUNTED,
            false,
            countedModel);
        configuration.put(configEnum, fault);
    }

    void addFaultSimple(ConfigEnum configEnum) {
        checkDuplicate(configEnum);
        ConfigFaultSimple fault = new ConfigFaultSimple(configEnum, ConfigFaultModel.FaultTypeEnum.SIMPLE, false);
        configuration.put(configEnum, fault);
    }

    public boolean testInsertFault(ConfigEnum configEnum) {
        ConfigBase configBase = lookupByEnum(configEnum);
        configBase.validateType(ConfigModel.ConfigTypeEnum.FAULT);
        ConfigFault fault = (ConfigFault) configBase;
        return fault.testInsertFault();
    }

    public void fault(ConfigEnum configEnum, FaultFunction fn) throws Exception {
        if (testInsertFault(configEnum)) {
            fn.apply();
        }
    }

    private ConfigBase lookup(String name) {
        ConfigEnum configEnum = ConfigEnum.lookupByApiName(name);
        return lookupByEnum(configEnum);
    }

    private ConfigBase lookupByEnum(ConfigEnum configEnum) {
        ConfigBase config = configuration.get(configEnum);
        if (config == null) {
            throw new ConfigNotFoundException("Unknown configuration name: " + configEnum.name());
        }
        return config;
    }

    private void checkDuplicate(ConfigEnum configEnum) {
        if (configuration.containsKey(configEnum)) {
            throw new DuplicateConfigNameException("Duplicate config name: " + configEnum.name());
        }
    }

    // -- Configuration Setup --

    // Setup the configuration. This is done once during construction.
    private void setConfiguration() {
        // -- Parameters --
        addParameter(SAM_RETRY_INITIAL_WAIT_SECONDS, samConfiguration.getRetryInitialWaitSeconds());
        addParameter(SAM_RETRY_MAXIMUM_WAIT_SECONDS, samConfiguration.getRetryMaximumWaitSeconds());
        addParameter(SAM_OPERATION_TIMEOUT_SECONDS, samConfiguration.getOperationTimeoutSeconds());
        addParameter(LOAD_BULK_ARRAY_FILES_MAX, appConfiguration.getMaxBulkFileLoadArray());
        addParameter(LOAD_BULK_FILES_MAX, appConfiguration.getMaxBulkFileLoad());
        addParameter(LOAD_CONCURRENT_FILES, appConfiguration.getLoadConcurrentFiles());
        addParameter(LOAD_CONCURRENT_INGESTS, appConfiguration.getLoadConcurrentIngests());
        addParameter(LOAD_DRIVER_WAIT_SECONDS, appConfiguration.getLoadDriverWaitSeconds());
        addParameter(LOAD_HISTORY_COPY_CHUNK_SIZE, appConfiguration.getLoadHistoryCopyChunkSize());
        addParameter(LOAD_HISTORY_WAIT_SECONDS, appConfiguration.getLoadHistoryWaitSeconds());
        addParameter(FIRESTORE_SNAPSHOT_BATCH_SIZE, appConfiguration.getFirestoreSnapshotBatchSize());
        addParameter(FIRESTORE_SNAPSHOT_CACHE_SIZE, appConfiguration.getFirestoreSnapshotCacheSize());
        addParameter(FIRESTORE_VALIDATE_BATCH_SIZE, appConfiguration.getFirestoreValidateBatchSize());
        addParameter(FIRESTORE_QUERY_BATCH_SIZE, appConfiguration.getFirestoreQueryBatchSize());

        // -- Faults --
        addFaultSimple(CREATE_ASSET_FAULT);
        addFaultCounted(SAM_TIMEOUT_FAULT, 0, -1, 25, ConfigFaultCountedModel.RateStyleEnum.FIXED);

        // Skip File Load fault is intended for bulk load infrastructure testing. It executes the bulk
        // load without copying any files. There is code in IngestFilePrimaryDataStep that
        // skips the copy and makes a dummy FSFileInfo. There is also code in DeleteDatasetPrimaryDataStep
        // that skips deleting files.
        addFaultSimple(LOAD_SKIP_FILE_LOAD);

        // Bucket resource lock faults. These are used by ResourceLockTest
        addFaultCounted(BUCKET_LOCK_CONFLICT_STOP_FAULT, 0, 1, 100, ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultSimple(BUCKET_LOCK_CONFLICT_CONTINUE_FAULT);

        // Dataset delete lock faults. These are used by DatasetConnectedTest > testOverlappingDeletes
        addFaultCounted(DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT, 0, 1, 100,
            ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultSimple(DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT);

        // Snapshot delete lock faults. These are used by SnapshotConnectedTest > testOverlappingDeletes
        addFaultCounted(SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT, 0, 1, 100,
            ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultSimple(SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT);

        // File ingest lock faults. These are used by DatasetConnectedTest > testSharedLockFileIngest
        addFaultCounted(FILE_INGEST_LOCK_CONFLICT_STOP_FAULT, 0, 2, 100,
            ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultSimple(FILE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT);

        // File Ingest - aquire shared lock fault. These are used by FileOperationTest > retryAndAcquireSharedLock
        addFaultSimple(FILE_INGEST_LOCK_FATAL_FAULT);
        addFaultSimple(FILE_INGEST_LOCK_RETRY_FAULT);
        addFaultSimple(FILE_INGEST_UNLOCK_FATAL_FAULT);
        addFaultSimple(FILE_INGEST_UNLOCK_RETRY_FAULT);

        // File delete lock faults. These are used by DatasetConnectedTest > testSharedLockFileDelete
        addFaultCounted(FILE_DELETE_LOCK_CONFLICT_STOP_FAULT, 0, 2, 100,
            ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultSimple(FILE_DELETE_LOCK_CONFLICT_CONTINUE_FAULT);

        // Table ingest lock faults. These are used by DatasetConnectedTest > testSharedLockTableIngest
        addFaultCounted(TABLE_INGEST_LOCK_CONFLICT_STOP_FAULT, 0, 2, 100,
            ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultSimple(TABLE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT);

        // soft delete lock faults. These are used by DatasetConnectedTest > testConcurrentSoftDeletes
        addFaultCounted(SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT, 0, 2, 100,
            ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultSimple(SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT);

        addFaultCounted(DATASET_GRANT_ACCESS_FAULT, 0, 3, 100, ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultCounted(SNAPSHOT_GRANT_ACCESS_FAULT, 0, 3, 100, ConfigFaultCountedModel.RateStyleEnum.FIXED);
        addFaultCounted(SNAPSHOT_GRANT_FILE_ACCESS_FAULT, 0, 3, 100, ConfigFaultCountedModel.RateStyleEnum.FIXED);
    }

}
