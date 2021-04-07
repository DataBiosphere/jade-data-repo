package bio.terra.service.upgrade;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Provides methods for upgrading the data repository metadata and stairway databases.
 * See <a href="https://docs.google.com/document/d/1CY9bOSwaw0HjdZ9uuxwm1rh4LkcOqV65tjI77IhKcxE/edit#">Liquibase
 * Migration Notes</a></a>
 */
@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "db.migrate")
public class MigrateConfiguration {
    private boolean dropAllOnStart;
    private boolean updateAllOnStart;
    private List<String> dataProjectNoDropAll;

    public boolean getDropAllOnStart() {
        return dropAllOnStart;
    }

    public void setDropAllOnStart(boolean dropAllOnStart) {
        this.dropAllOnStart = dropAllOnStart;
    }

    public boolean getUpdateAllOnStart() {
        return updateAllOnStart;
    }

    public void setUpdateAllOnStart(boolean updateAllOnStart) {
        this.updateAllOnStart = updateAllOnStart;
    }

    public List<String> getDataProjectNoDropAll() {
        return dataProjectNoDropAll;
    }

    public void setDataProjectNoDropAll(List<String> dataProjectNoDropAll) {
        this.dataProjectNoDropAll = dataProjectNoDropAll;
    }
}
