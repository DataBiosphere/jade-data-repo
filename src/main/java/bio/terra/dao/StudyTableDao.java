package bio.terra.dao;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class StudyTableDao extends TableDaoBase {
    private final DataRepoJdbcConfiguration dataRepoJdbcConfiguration;

    @Autowired
    public StudyTableDao(DataRepoJdbcConfiguration dataRepoJdbcConfiguration) {
        super(dataRepoJdbcConfiguration,
            "study_table",
            "study_column",
            "study_id");
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
    }
}
