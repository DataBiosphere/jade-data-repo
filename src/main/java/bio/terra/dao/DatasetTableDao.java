package bio.terra.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DatasetTableDao extends TableDaoBase {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public DatasetTableDao(NamedParameterJdbcTemplate jdbcTemplate) {
        super(jdbcTemplate, "dataset_table", "dataset_column", "dataset_id");
        this.jdbcTemplate = jdbcTemplate;
    }
}
