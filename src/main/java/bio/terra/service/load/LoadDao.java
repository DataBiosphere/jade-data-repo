package bio.terra.service.load;

import bio.terra.common.DaoKeyHolder;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class LoadDao {
  private final Logger logger = LoggerFactory.getLogger(LoadDao.class);

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public LoadDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  // -- load tags public methods --

  // This must be serializable so that conflicting updates of the locked state and flightid
  // are detected. We lock the table so that we avoid serialization errors.

  /**
   * We implement a rule that one load job can use one load tag at a time. That rule is needed to
   * control concurrent operations. For example, a delete-by-load-tag cannot compete with a load;
   * two loads cannot run in parallel with the same load tag - it confuses the algorithm for
   * re-running a load with a load tag and skipping already-loaded files.
   *
   * <p>This call and the unlock call use a load table in the database to record that a load tag is
   * in use. The load tag is associated with a load id (a guid); that guid is a foreign key to the
   * load_file table that maintains the state of files being loaded.
   *
   * <p>We expect conflicts on load tags to be rare. The typical case will be: a load starts, runs,
   * and ends without conflict and with a re-run.
   *
   * <p>We learned from the first implementation of this code that when there were conflicts, we
   * would get serialization errors from Postgres. Those require building retry logic. Instead, we
   * chose to use table locks to serialize access to the load table during the time we are setting
   * and freeing the our load lock state.
   *
   * <p>A lock is taken by creating the load tag row and storing the flight id holding the lock. The
   * lock is freed by deleting the load tag row. Code can safely re-lock a load tag lock it holds
   * and unlock a load tag lock it has freed.
   *
   * <p>There is never a case where a lock row is updated. They are only ever inserted or deleted.
   *
   * @param loadTag tag identifying this load
   * @param flightId flight id taking the lock
   * @return Load object including the load id
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public Load lockLoad(String loadTag, String flightId) throws InterruptedException {
    jdbcTemplate.getJdbcTemplate().execute("LOCK TABLE load IN EXCLUSIVE MODE");

    String upsert =
        "INSERT INTO load (load_tag, locked, locking_flight_id)"
            + " VALUES (:load_tag, true, :flight_id)"
            + " ON CONFLICT ON CONSTRAINT load_load_tag_key DO NOTHING";

    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("load_tag", loadTag).addValue("flight_id", flightId);
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    int rows = jdbcTemplate.update(upsert, params, keyHolder);
    Load load;
    if (rows == 0) {
      // We did not insert. Therefore, someone has the load tag locked.
      // Retrieve it, in case it is us re-locking
      load = lookupLoadByTag(loadTag);
      if (load == null) {
        throw new CorruptMetadataException("Load row should exist! Load tag: " + loadTag);
      }

      // It is locked by someone else
      if (!StringUtils.equals(load.getLockingFlightId(), flightId)) {
        throw new LoadLockedException(
            "Load '" + loadTag + "' is locked by flight '" + load.getLockingFlightId() + "'");
      }
    } else {
      load =
          new Load()
              .id(keyHolder.getId())
              .loadTag(keyHolder.getString("load_tag"))
              .locked(keyHolder.getField("locked", Boolean.class).orElse(false))
              .lockingFlightId(keyHolder.getString("locking_flight_id"));
    }

    return load;
  }

  /**
   * Unlocking means deleting the specific row for our load tag and flight id. If no row is deleted,
   * we assume this is a redo of the unlock and is OK. That can happen if a flight successfully
   * unlocks and then fails. When the first flight recovers it will retry unlocking. We don't want
   * that to be an error.
   *
   * <p>If a row is deleted, then we have performed the unlock. So we don't check the affected row
   * count.
   *
   * @param loadTag tag identifying this load
   * @param flightId flight id releasing the lock
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void unlockLoad(String loadTag, String flightId) {
    jdbcTemplate.getJdbcTemplate().execute("LOCK TABLE load IN EXCLUSIVE MODE");

    String delete =
        "DELETE FROM load WHERE load_tag = :load_tag AND locking_flight_id = :flight_id";
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("load_tag", loadTag).addValue("flight_id", flightId);
    jdbcTemplate.update(delete, params);
  }

  private Load lookupLoadByTag(String loadTag) {
    String sql =
        "SELECT id, load_tag, locked, locking_flight_id FROM load WHERE load_tag = :load_tag";
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("load_tag", loadTag);
    return jdbcTemplate.queryForObject(
        sql,
        params,
        (rs, rowNum) ->
            new Load()
                .id(rs.getObject("id", UUID.class))
                .loadTag(rs.getString("load_tag"))
                .locked(rs.getBoolean("locked"))
                .lockingFlightId(rs.getString("locking_flight_id")));
  }

  // -- load files methods --

  // TODO: REVIEWERS: note that counter to our usual practice, I am using the interface datatype
  // instead of
  //  insulating the innards from the interface. That seems justified in this case, because I don't
  // want to
  //  use memory making another big array.
  //  Also, I am a tiny bit concerned about insert performance. This code is implemented in the
  // obvious
  //  way using JdbcTemplate. JdbcTemplate documentation says that it uses JDBC batchUpdate if it is
  // supported
  //  by the underlying database system. However, some posts in StackOverflow claim that it is not
  // using the
  //  batch, but issuing an insert for each row.
  //  I created DR-738 to track performance measurement of file load to decide if this method should
  // get
  //  more clever.

  // Insert one batch of file load instructions into the load_file table
  @Transactional
  public void populateFiles(UUID loadId, List<BulkLoadFileModel> loadFileModelList) {
    final String sql =
        """
        INSERT INTO load_file
        (load_id, source_path, target_path, mime_type, description, state, checksum_md5)
        VALUES(?,?,?,?,?,?,?)
        """;

    JdbcTemplate baseJdbcTemplate = jdbcTemplate.getJdbcTemplate();
    baseJdbcTemplate.batchUpdate(
        sql,
        new BatchPreparedStatementSetter() {
          public void setValues(@NotNull PreparedStatement ps, int i) throws SQLException {
            ps.setObject(1, loadId);
            ps.setString(2, loadFileModelList.get(i).getSourcePath());
            ps.setString(3, loadFileModelList.get(i).getTargetPath());
            ps.setString(4, loadFileModelList.get(i).getMimeType());
            ps.setString(5, loadFileModelList.get(i).getDescription());
            ps.setString(6, BulkLoadFileState.NOT_TRIED.toString());
            ps.setString(7, loadFileModelList.get(i).getMd5());
          }

          public int getBatchSize() {
            return loadFileModelList.size();
          }
        });
  }

  /**
   * Chunk a stream of models into groups of size batchSize and populate the database
   *
   * @param loadId Load ID tying all these file ingests together
   * @param loadFileModelStream The stream to be chunked and processed over
   */
  @Transactional
  public void populateFiles(
      UUID loadId, Stream<BulkLoadFileModel> loadFileModelStream, int batchSize) {
    Spliterator<BulkLoadFileModel> split = loadFileModelStream.spliterator();

    while (true) {
      List<BulkLoadFileModel> batch = new ArrayList<>(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (!split.tryAdvance(batch::add)) {
          break;
        }
      }
      if (batch.isEmpty()) {
        break;
      }
      populateFiles(loadId, batch);
    }
  }

  // Remove all file load instructions for a given loadId from the load_file table
  public void cleanFiles(UUID loadId) {
    jdbcTemplate.update(
        "DELETE FROM load_file WHERE load_id = :load_id",
        new MapSqlParameterSource().addValue("load_id", loadId));
  }

  public List<LoadFile> findLoadsByState(UUID loadId, BulkLoadFileState state, Integer limit) {
    return queryByState(loadId, state, limit);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public LoadCandidates findCandidates(UUID loadId, int candidatesToFind) {
    final String countFailedSql =
        "SELECT count(*) AS failed FROM load_file" + " WHERE load_id = :load_id AND state = :state";
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("load_id", loadId);
    params.addValue("state", BulkLoadFileState.FAILED.toString());
    Integer failedFiles = jdbcTemplate.queryForObject(countFailedSql, params, Integer.class);
    if (failedFiles == null) {
      failedFiles = 0;
    }

    List<LoadFile> runningLoads = queryByState(loadId, BulkLoadFileState.RUNNING, null);
    List<LoadFile> candidateFiles =
        queryByState(loadId, BulkLoadFileState.NOT_TRIED, candidatesToFind);

    return new LoadCandidates()
        .runningLoads(runningLoads)
        .candidateFiles(candidateFiles)
        .failedLoads(failedFiles);
  }

  @Transactional
  public List<LoadFile> getFailedLoads(UUID loadId, int maxResults) {
    return queryByState(loadId, BulkLoadFileState.FAILED, maxResults);
  }

  public void setLoadFileNotTried(UUID loadId, String targetPath) {
    updateLoadFile(loadId, targetPath, BulkLoadFileState.NOT_TRIED, null, null, null, null);
  }

  public void setLoadFileRunning(UUID loadId, String targetPath, String flightId) {
    updateLoadFile(loadId, targetPath, BulkLoadFileState.RUNNING, null, null, null, flightId);
  }

  public void setLoadFileSucceeded(
      UUID loadId, String targetPath, String fileId, FSFileInfo fileInfo) {
    updateLoadFile(loadId, targetPath, BulkLoadFileState.SUCCEEDED, fileId, fileInfo, null, null);
  }

  public void setLoadFileFailed(UUID loadId, String targetPath, String error) {
    updateLoadFile(loadId, targetPath, BulkLoadFileState.FAILED, null, null, error, null);
  }

  public BulkLoadResultModel makeBulkLoadResult(UUID loadId) {
    final String bulkLoadResultSql =
        "SELECT state, count(*) AS statecount FROM load_file"
            + " WHERE load_id = :load_id GROUP BY state";
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("load_id", loadId);
    // Note: the cast of `rs` is needed because ResultSetExtractor and RowCallbackHandler have
    // duplicate signatures
    return jdbcTemplate.query(
        bulkLoadResultSql,
        params,
        rs -> {
          BulkLoadResultModel result =
              new BulkLoadResultModel()
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
              case RUNNING -> {
                logger.info("Unexpected running loads: " + rs.getInt("statecount"));
                throw new CorruptMetadataException("No loads should be running!");
              }
              case FAILED -> result.setFailedFiles(rs.getInt("statecount"));
              case NOT_TRIED -> result.setNotTriedFiles(rs.getInt("statecount"));
              case SUCCEEDED -> result.setSucceededFiles(rs.getInt("statecount"));
              default -> throw new CorruptMetadataException("Invalid load state");
            }
            result.setTotalFiles(
                result.getFailedFiles() + result.getNotTriedFiles() + result.getSucceededFiles());
          }
          return result;
        });
  }

  public List<BulkLoadFileResultModel> makeBulkLoadFileArray(UUID loadId) {
    final String sql =
        "SELECT source_path, target_path, state, file_id, error"
            + " FROM load_file WHERE load_id = :load_id";
    return jdbcTemplate.query(
        sql,
        new MapSqlParameterSource().addValue("load_id", loadId),
        (rs, rowNum) ->
            new BulkLoadFileResultModel()
                .sourcePath(rs.getString("source_path"))
                .targetPath(rs.getString("target_path"))
                .state(BulkLoadFileState.fromValue(rs.getString("state")))
                .fileId(rs.getString("file_id"))
                .error(rs.getString("error")));
  }

  public Integer bulkLoadFileArraySize(UUID loadId) {
    final String sql = "SELECT count(*) FROM load_file WHERE load_id = :load_id";
    return jdbcTemplate.queryForObject(
        sql, new MapSqlParameterSource().addValue("load_id", loadId), Integer.class);
  }

  public List<BulkLoadHistoryModel> makeLoadHistoryArray(UUID loadId, int chunkSize, int chunkNum) {
    final String sql =
        "SELECT source_path, target_path, state, file_id, checksum_crc32c, checksum_md5, error"
            + " FROM load_file WHERE load_id = :load_id"
            + " ORDER BY file_id"
            + " LIMIT :chunk_size OFFSET :chunk_offset";
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("load_id", loadId);
    params.addValue("chunk_size", chunkSize);
    params.addValue("chunk_offset", chunkNum * chunkSize);
    return jdbcTemplate.query(
        sql,
        params,
        (rs, rowNum) ->
            new BulkLoadHistoryModel()
                .sourcePath(rs.getString("source_path"))
                .targetPath(rs.getString("target_path"))
                .state(BulkLoadFileState.fromValue(rs.getString("state")))
                .fileId(rs.getString("file_id"))
                .checksumCRC(rs.getString("checksum_crc32c"))
                .checksumMD5(rs.getString("checksum_md5"))
                .error(rs.getString("error")));
  }

  // -- private methods --
  private List<LoadFile> queryByState(UUID loadId, BulkLoadFileState state, Integer limit) {
    String sql =
        """
      SELECT source_path, target_path, mime_type, description, state, flight_id, file_id, error,
      checksum_md5
      FROM load_file WHERE load_id = :load_id AND state = :state
      """;
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("load_id", loadId).addValue("state", state.toString());

    if (limit != null) {
      sql = sql + " LIMIT :limit";
      params.addValue("limit", limit);
    }

    return jdbcTemplate.query(
        sql,
        params,
        (rs, rowNum) ->
            new LoadFile()
                .loadId(loadId)
                .sourcePath(rs.getString("source_path"))
                .targetPath(rs.getString("target_path"))
                .mimeType(rs.getString("mime_type"))
                .description(rs.getString("description"))
                .state(BulkLoadFileState.fromValue(rs.getString("state")))
                .flightId(rs.getString("flight_id"))
                .fileId(rs.getString("file_id"))
                .error(rs.getString("error"))
                .md5(rs.getString("checksum_md5")));
  }

  private void updateLoadFile(
      UUID loadId,
      String targetPath,
      BulkLoadFileState state,
      String fileId,
      FSFileInfo fileInfo,
      String error,
      String flightId) {
    String checksumCRC = null;
    String checksumMD5 = null;
    if (fileInfo != null) {
      checksumCRC = fileInfo.getChecksumCrc32c();
      checksumMD5 = fileInfo.getChecksumMd5();
    }
    final String sql =
        "UPDATE load_file"
            + " SET state = :state, file_id = :file_id, flight_id = :flight_id,"
            + " checksum_crc32c = :checksum_crc, checksum_md5 = :checksum_md5,"
            + " error = :error"
            + " WHERE load_id = :load_id AND target_path = :target_path";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("state", state.toString())
            .addValue("flight_id", flightId)
            .addValue("file_id", fileId)
            .addValue("checksum_crc", checksumCRC)
            .addValue("checksum_md5", checksumMD5)
            .addValue("error", error)
            .addValue("load_id", loadId)
            .addValue("target_path", targetPath);
    jdbcTemplate.update(sql, params);
  }
}
