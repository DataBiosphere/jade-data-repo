package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class DatasetTableDao extends TableDaoBase {
    private final DataRepoJdbcConfiguration dataRepoJdbcConfiguration;

    @Autowired
    public DatasetTableDao(DataRepoJdbcConfiguration dataRepoJdbcConfiguration) {
        super(dataRepoJdbcConfiguration, "dataset_table", "dataset_column", "dataset_id");
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
    }
}
