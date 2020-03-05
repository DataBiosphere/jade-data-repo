package bio.terra.service.load;


import bio.terra.common.DaoKeyHolder;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Repository
public class LoadDao {
    private final Logger logger = LoggerFactory.getLogger(LoadDao.class);

    private NamedParameterJdbcTemplate jdbcTemplate;
    private ConfigurationService configService;

    @Autowired
    public LoadDao(NamedParameterJdbcTemplate jdbcTemplate, ConfigurationService configService) {
        this.jdbcTemplate = jdbcTemplate;
        this.configService = configService;
    }

    // -- load tags public methods --

    // This must be serializable so that conflicting updates of the locked state and flightid
    // are detected. The default isolation level for Postgres is READ_COMMITTED; that will allow
    // the second updater to overwrite the first updater's data. Serialization means the second
    // updater will throw a PSQLException.
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.SERIALIZABLE)
    public Load lockLoad(String loadTag, String flightId) {
        Load load = lookupLoadByTag(loadTag);
        if (load == null) {
            load = createLoad(loadTag, flightId);
        }

        if (load.isLocked()) {
            if (StringUtils.equals(flightId, load.getLockingFlightId())) {
                System.out.println("SUCCESS: we have it locked");
                // we already have it locked
                return load;
            } else {
                System.out.println("CONFLICT: already locked");
                // another flight has it locked
                conflictThrow(load);
            }
        }

        // FAULT: see LoadDaoUnitTest for the rationale for this code.
        if (configService.testInsertFault(ConfigEnum.LOAD_LOCK_CONFLICT_STOP_FAULT)) {
            try {
                logger.info("LOAD_LOCK_CONFLICT_STOP");
                while (!configService.testInsertFault(ConfigEnum.LOAD_LOCK_CONFLICT_CONTINUE_FAULT)) {
                    logger.info("Sleeping for CONTINUE FAULT");
                    TimeUnit.SECONDS.sleep(2);
                }
                logger.info("LOAD_LOCK_CONFLICT_CONTINUE");
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new LoadLockFailureException("Unexpected interrupt during load lock fault");
            }
        }

        try {
            // Lock the load for our use and validate
            updateLoad(load.getId(), true, flightId);
        } catch (CannotSerializeTransactionException ex) {
            conflictThrow(load);
        }

        load = lookupLoadByTag(loadTag);
        if (load != null && load.isLocked() && StringUtils.equals(load.getLockingFlightId(), flightId)) {
            return load;
        }
        throw new LoadLockFailureException("Internal error: failed to lock a load!");
    }

    // To support idempotent steps, this method will not complain if the load no longer exists or if
    // it is already unlocked. It throws if the lock is held by a different flight or if the unlock operation
    // fails for some reason.
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public void unlockLoad(String loadTag, String flightId) {
        Load load = lookupLoadByTag(loadTag);
        if (load == null || !load.isLocked()) {
            return; // nothing to unlock
        }

        if (!StringUtils.equals(load.getLockingFlightId(), flightId)) {
            conflictThrow(load);
        }
        updateLoad(load.getId(), false, null);
        load = lookupLoadByTag(loadTag);
        if (load == null || load.isLocked() || load.getLockingFlightId() != null) {
            throw new LoadLockFailureException("Internal error: failed to unlock a load!");
        }
    }

    // -- load files methods --

    // TODO: REVIEWERS: note that counter to our usual practice, I am using the interface datatype instead of
    //  insulating the innards from the interface. That seems justified in this case, because I don't want to
    //  use memory making another big array.
    //  Also, I am a tiny bit concerned about insert performance. This code is implemented in the obvious
    //  way using JdbcTemplate. JdbcTemplate documentation says that it uses JDBC batchUpdate if it is supported
    //  by the underlying database system. However, some posts in StackOverflow claim that it is not using the
    //  batch, but issuing an insert for each row.
    //  I created DR-738 to track performance measurement of file load to decide if this method should get
    //  more clever.

    // Insert one batch of file load instructions into the load_file table
    @Transactional
    public void populateFiles(UUID loadId, List<BulkLoadFileModel> loadFileModelList) {
        final String sql = "INSERT INTO load_file " +
            " (load_id, source_path, target_path, mime_type, description, state)" +
            " VALUES(?,?,?,?,?,?)";

        JdbcTemplate baseJdbcTemplate = jdbcTemplate.getJdbcTemplate();
        baseJdbcTemplate.batchUpdate(sql,
            new BatchPreparedStatementSetter() {
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setObject(1, loadId);
                    ps.setString(2, loadFileModelList.get(i).getSourcePath());
                    ps.setString(3, loadFileModelList.get(i).getTargetPath());
                    ps.setString(4, loadFileModelList.get(i).getMimeType());
                    ps.setString(5, loadFileModelList.get(i).getDescription());
                    ps.setString(6, BulkLoadFileState.NOT_TRIED.toString());
                }

                public int getBatchSize() {
                    return loadFileModelList.size();
                }
            });
    }

    // Remove all file load instructions for a given loadId from the load_file table
    public void cleanFiles(UUID loadId) {
        jdbcTemplate.update("DELETE FROM load_file WHERE load_id = :load_id",
                new MapSqlParameterSource().addValue("load_id", loadId));
    }

    public List<LoadFile> findLoadsByState(UUID loadId, BulkLoadFileState state, Integer limit) {
        return queryByState(loadId, state, limit);
    }

    public LoadCandidates findCandidates(UUID loadId, int candidatesToFind) {
        final String countFailedSql = "SELECT count(*) AS failed FROM load_file" +
            " WHERE load_id = :load_id AND state = :state";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("load_id", loadId);
        params.addValue("state", BulkLoadFileState.FAILED.toString());
        Integer failedFiles = jdbcTemplate.queryForObject(countFailedSql, params, Integer.class);
        if (failedFiles == null) {
            failedFiles = 0;
        }

        List<LoadFile> runningLoads = queryByState(loadId, BulkLoadFileState.RUNNING, null);
        List<LoadFile> candidateFiles = queryByState(loadId, BulkLoadFileState.NOT_TRIED, candidatesToFind);

        return new LoadCandidates()
            .runningLoads(runningLoads)
            .candidateFiles(candidateFiles)
            .failedLoads(failedFiles);
    }

    public void setLoadFileNotTried(UUID loadId, String targetPath) {
        updateLoadFile(loadId, targetPath, BulkLoadFileState.NOT_TRIED, null, null, null);
    }

    public void setLoadFileRunning(UUID loadId, String targetPath, String flightId) {
        updateLoadFile(loadId, targetPath, BulkLoadFileState.RUNNING, null, null, flightId);
    }

    public void setLoadFileSucceeded(UUID loadId, String targetPath, String fileId) {
        updateLoadFile(loadId, targetPath, BulkLoadFileState.SUCCEEDED, fileId, null, null);
    }

    public void setLoadFileFailed(UUID loadId, String targetPath, String error) {
        updateLoadFile(loadId, targetPath, BulkLoadFileState.FAILED, null, error, null);
    }

    public BulkLoadResultModel makeBulkLoadResult(UUID loadId) {
        final String bulkLoadResultSql = "SELECT state, count(*) AS statecount FROM load_file" +
            " WHERE load_id = :load_id GROUP BY state";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("load_id", loadId);
        // Note: the cast of `rs` is needed because ResultSetExtractor and RowCallbackHandler have duplicate signatures
        return jdbcTemplate.query(bulkLoadResultSql, params,
            (ResultSetExtractor<BulkLoadResultModel>) rs -> {
                BulkLoadResultModel result = new BulkLoadResultModel()
                    .succeededFiles(0)
                    .failedFiles(0)
                    .notTriedFiles(0)
                    .totalFiles(0);

                while (rs.next()) {
                    BulkLoadFileState state = BulkLoadFileState.fromValue(rs.getString("state"));
                    if (state == null) {
                        throw new CorruptMetadataException("Invalid file state");
                    }
                    switch (state) {
                        case RUNNING:
                            throw new CorruptMetadataException("No loads should be running!");

                        case FAILED:
                            result.setFailedFiles(rs.getInt("statecount"));
                            break;

                        case NOT_TRIED:
                            result.setNotTriedFiles(rs.getInt("statecount"));
                            break;

                        case SUCCEEDED:
                            result.setSucceededFiles(rs.getInt("statecount"));
                            break;
                    }
                    result.setTotalFiles(result.getFailedFiles() +
                        result.getNotTriedFiles() +
                        result.getSucceededFiles());
                }
                return result;
            });
    }

    public List<BulkLoadFileResultModel> makeBulkLoadFileArray(UUID loadId) {
        final String sql = "SELECT source_path, target_path, state, file_id, error" +
            " FROM load_file WHERE load_id = :load_id";
        return jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("load_id", loadId),
            (rs, rowNum) -> {
                return new BulkLoadFileResultModel()
                    .sourcePath(rs.getString("source_path"))
                    .targetPath(rs.getString("target_path"))
                    .state(BulkLoadFileState.fromValue(rs.getString("state")))
                    .fileId(rs.getString("file_id"))
                    .error(rs.getString("error"));
            });
    }

    // -- private methods --
    private List<LoadFile> queryByState(UUID loadId, BulkLoadFileState state, Integer limit) {
        String sql = "SELECT source_path, target_path, mime_type, description, state, flight_id, file_id, error" +
            " FROM load_file WHERE load_id = :load_id AND state = :state";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("load_id", loadId)
            .addValue("state", state.toString());

        if (limit != null) {
            sql = sql + " LIMIT :limit";
            params.addValue("limit", limit);
        }

        return jdbcTemplate.query(sql, params, (rs, rowNum) ->
            new LoadFile()
                .loadId(loadId)
                .sourcePath(rs.getString("source_path"))
                .targetPath(rs.getString("target_path"))
                .mimeType(rs.getString("mime_type"))
                .description(rs.getString("description"))
                .state(BulkLoadFileState.fromValue(rs.getString("state")))
                .flightId(rs.getString("flight_id"))
                .fileId(rs.getString("file_id"))
                .error(rs.getString("error")));
    }

    private void updateLoadFile(UUID loadId,
                                String targetPath,
                                BulkLoadFileState state,
                                String fileId,
                                String error,
                                String flightId) {
        final String sql = "UPDATE load_file" +
            " SET state = :state, file_id = :file_id, error = :error, flight_id = :flight_id" +
            " WHERE load_id = :load_id AND target_path = :target_path";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("state", state.toString())
            .addValue("flight_id", flightId)
            .addValue("file_id", fileId)
            .addValue("error", error)
            .addValue("load_id", loadId)
            .addValue("target_path", targetPath);
        jdbcTemplate.update(sql, params);
    }

    private void conflictThrow(Load load) {
        throw new LoadLockedException("Load " + load.getLoadTag() +
            " is locked by flight " + load.getLockingFlightId());
    }

    // Creates a new load and locks it on behalf of this flight
    private Load createLoad(String loadTag, String flightId) {
        String sql = "INSERT INTO load (load_tag, locked, locking_flight_id)" +
            " VALUES (:load_tag, true, :flight_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("load_tag", loadTag)
            .addValue("flight_id", flightId);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        Load load = new Load()
            .id(keyHolder.getId())
            .loadTag(keyHolder.getString("load_tag"))
            .locked(keyHolder.getField("locked", Boolean.class))
            .lockingFlightId(keyHolder.getString("locking_flight_id"));

        return load;
    }

    // Updates the load to either lock or unlock it
    @Transactional(propagation = Propagation.MANDATORY, isolation = Isolation.SERIALIZABLE)
    private void updateLoad(UUID loadId, boolean lock, String flightId) {
        String sql = "UPDATE load SET locked = :locked, locking_flight_id = :flight_id WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("locked", lock)
            .addValue("flight_id", flightId)
            .addValue("id", loadId);
        jdbcTemplate.update(sql, params);
    }

    // Returns null if not found
    private Load lookupLoadByTag(String loadTag) {
        try {
            String sql = "SELECT * FROM load WHERE load_tag = :load_tag";
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("load_tag", loadTag);
            Load load = jdbcTemplate.queryForObject(sql, params, (rs, rowNum) ->
                new Load()
                    .id(rs.getObject("id", UUID.class))
                    .loadTag(rs.getString("load_tag"))
                    .locked(rs.getBoolean("locked"))
                    .lockingFlightId(rs.getString("locking_flight_id")));
            return load;
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

}
