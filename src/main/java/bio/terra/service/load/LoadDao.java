package bio.terra.service.load;


import bio.terra.common.DaoKeyHolder;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.service.load.exception.LoadLockedException;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Repository
public class LoadDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.snapshot.LoadDao");

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public LoadDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public Load lockLoad(String loadTag, String flightId) {
        Load load = lookupLoadByTag(loadTag);
        if (load == null) {
            load = createLoad(loadTag, flightId);
        }

        if (load.isLocked()) {
            if (StringUtils.equals(flightId, load.getLockingFlightId())) {
                // we already have it locked
                return load;
            } else {
                // another flight has it locked
                conflictThrow(load);
            }
        }

        // Lock the load for our use and validate
        updateLoad(load.getId(), true, flightId);
        load = lookupLoadByTag(loadTag);
        if (load != null && load.isLocked() && StringUtils.equals(load.getLockingFlightId(), flightId)) {
            return load;
        }
        throw new LoadLockFailureException("Internal error: failed to lock a load!");
    }

    // To support idempotent steps, this method will not complain if the load no longer exists or if
    // it is already unlocked. It throws if the lock is held by a different flight or if the unlock operation
    // fails for some reason.
    @Transactional(propagation = Propagation.REQUIRED)
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
