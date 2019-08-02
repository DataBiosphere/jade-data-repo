package bio.terra.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshotTableDao extends TableDaoBase {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public SnapshotTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        super(jdbcTemplate, "snapshot_table", "snapshot_column", "parent_id");
        this.jdbcTemplate = jdbcTemplate;
    }
}
