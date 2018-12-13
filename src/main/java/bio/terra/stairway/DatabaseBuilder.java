package bio.terra.stairway;

import javax.sql.DataSource;

/**
 * Builder pattern implementation for Database object.
 */
public class DatabaseBuilder {
    private DataSource dataSource = null;
    private boolean forceCleanStart = false;
    private String schemaName = null;
    private String nameStem = null;

    public DatabaseBuilder dataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public DatabaseBuilder forceCleanStart(boolean forceCleanStart) {
        this.forceCleanStart = forceCleanStart;
        return this;
    }

    public DatabaseBuilder schemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public DatabaseBuilder nameStem(String nameStem) {
        this.nameStem = nameStem;
        return this;
    }

    public Database build() {
        return new Database(dataSource, forceCleanStart, schemaName, nameStem);
    }

}
