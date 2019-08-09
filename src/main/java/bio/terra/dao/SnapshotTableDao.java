package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshotTableDao extends TableDaoBase {
    private final DataRepoJdbcConfiguration dataRepoJdbcConfiguration;

    @Autowired
    public SnapshotTableDao(DataRepoJdbcConfiguration dataRepoJdbcConfiguration) {
        super(dataRepoJdbcConfiguration, "snapshot_table", "snapshot_column", "parent_id");
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
    }
}
