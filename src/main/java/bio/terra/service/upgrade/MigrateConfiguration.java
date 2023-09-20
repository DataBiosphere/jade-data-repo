package bio.terra.service.upgrade;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

/**
 * Provides methods for upgrading the data repository metadata and stairway databases. See <a
 * href="https://docs.google.com/document/d/1CY9bOSwaw0HjdZ9uuxwm1rh4LkcOqV65tjI77IhKcxE/edit#">Liquibase
 * Migration Notes</a></a>
 */
@ConfigurationProperties(prefix = "db.migrate")
@ConstructorBinding
public record MigrateConfiguration(boolean dropAllOnStart, boolean updateAllOnStart) {}
