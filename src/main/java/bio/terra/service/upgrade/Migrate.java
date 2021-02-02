package bio.terra.service.upgrade;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.kubernetes.KubeService;
import bio.terra.service.upgrade.exception.MigrateException;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.commons.lang.StringUtils;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Provides methods for upgrading the data repository metadata and stairway databases.
 * See <a href="https://docs.google.com/document/d/1CY9bOSwaw0HjdZ9uuxwm1rh4LkcOqV65tjI77IhKcxE/edit#">Liquibase
 * Migration Notes</a></a>
 * <p>
 * The algorithm here is:
 * - check if the deployment table exists. If it doesn't exist, create it.
 * - get the deployment uid from KubeService. If we are in Kubernetes, this will be a real uid of a real
 * deployment. If not in Kubernetes, it will be based on a random UUID.
 * - take a postgres lock on the deployment table to serialize instances running this algorithm
 * - read the deployment row
 * - If the id = the KubeService id, we return false - we are not the first through with that deployment id.
 * - If the id != the KubeService id and the row is locked, we spin until the row is unlocked and return false.
 * TODO: validate that the locker is still alive by listing pods
 * - If the id != the KubeService id and the row is unlocked:
 * -- update the deployment row with the new deployment and mark that this pod owns the lock
 * -- perform the migrate according to the configuration settings
 * -- return true
 * We rely on JobService to call back in to:
 * -- update the deployment row, unlocking the row and releasing any waiting DRmanagers
 * That is because we need to hold the migration lock during Stairway migration.
 * <p>
 * This is vulnerable to failure: if this pod crashes still holding the deployment lock, we will be stuck and
 * have to clear it by hand.
 * <p>
 * If we decide that is an important case to cover, we can fix it by doing yet another check that the lock holder
 * is on the list of running pods. For now, not gonna do it.
 * <p>
 * NOTE: you might be wondering why this code does not use JdbcTemplate. I wonder why also.
 * I initially coded it with JdbcTemplate and the @Transactional annotation got the error:
 * PSQLException: ERROR: LOCK TABLE can only be used in transaction blocks
 * I tried a number of solutions and learned some Spring quirks on the way. My guess is that it has
 * something to do with the timing of the PlatformTransactionManager configuration with respect to
 * execution of the StartupInitializer. In the end, I never solved the problem.
 * I converted the code to use java.sql directly with explicit transactions.
 */
@Component
public class Migrate {
    private static final Logger logger = LoggerFactory.getLogger("bio.terra.service.upgrade");
    private final DataRepoJdbcConfiguration dataRepoJdbcConfiguration;
    private final MigrateConfiguration migrateConfiguration;
    private final KubeService kubeService;
    private final DataSource dataSource;

    private enum MigrateAction {
        NOTHING,
        WAIT_FOR_UNLOCK,
        MIGRATE
    }

    private enum WaitState {
        KEEP_WAITING,
        DONE_WAITING,
        RETRY_MIGRATE
    }

    private static class DeploymentRow {
        private String id;
        private String lockingPodName;

        public String getId() {
            return id;
        }

        public DeploymentRow id(String id) {
            this.id = id;
            return this;
        }

        public String getLockingPodName() {
            return lockingPodName;
        }

        public DeploymentRow lockingPodName(String lockingPodName) {
            this.lockingPodName = lockingPodName;
            return this;
        }
    }

    @Autowired
    public Migrate(DataRepoJdbcConfiguration dataRepoJdbcConfiguration,
                   MigrateConfiguration migrateConfiguration,
                   KubeService kubeService) {
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
        this.migrateConfiguration = migrateConfiguration;
        this.kubeService = kubeService;
        this.dataSource = dataRepoJdbcConfiguration.getDataSource();
    }

    /**
     * Decide whether to perform migration
     *
     * @return true if we did the migration; false otherwise
     */
    public boolean migrateDatabase() {
        // Make sure the deployment table exists
        makeDeploymentTable();

        // Get the unique id of this deployment
        String deploymentUid = kubeService.getApiDeploymentUid();

        // Figure out what we should do, based on the state. This is in a loop so that if the deployment
        // lock is stuck, we can manually clear it and one of the waiting-for-unlock DRmanagers can
        // perform migration.

        boolean retry = true;
        while (retry) {
            MigrateAction action = checkDeploymentState(deploymentUid);
            switch (action) {
                case NOTHING:
                    logger.info("Deployment id matches - no action taken");
                    return false;

                case MIGRATE:
                    logger.info("Deployment locked deployment - migrating");
                    migrateDatabase(
                        dataRepoJdbcConfiguration.getChangesetFile(),
                        dataRepoJdbcConfiguration.getDataSource());
                    return true;

                case WAIT_FOR_UNLOCK:
                    retry = waitForUnlock(deploymentUid);
                    break;
            }
        }
        return false;
    }

    /**
     * We need to separate releasing of the migrate lock from the main migrate processing, so that
     * JobService can initialize Stairway and cause its databases to be migrated, BEFORE
     * we release the lock.
     * <p>
     * So if the migrateAllDatabases returns true, then the caller MUST call this.
     */
    public void releaseMigrateLock() {
        logger.info("Deployment unlocking deployment");
        releaseDeploymentLock();
    }

    /**
     * Wait for another DRmanager to perform the migration.
     *
     * @param deploymentUid deployment id we are trying to upgrade to
     * @return true if this DRmanager should retry the migration. False if migration succeeded.
     */
    public boolean waitForUnlock(String deploymentUid) {
        try {
            WaitState result = WaitState.KEEP_WAITING;
            while (result == WaitState.KEEP_WAITING) {
                logger.info("Deployment locked - waiting");
                TimeUnit.SECONDS.sleep(5);
                result = tryUnlock(deploymentUid);
            }
            return (result == WaitState.RETRY_MIGRATE);
        } catch (InterruptedException ex) {
            throw new MigrateException("Interrupted waiting for migration to complete", ex);
        }
    }

    // Three state return:
    // - KEEP_WAITING - deployment row is locked
    // - DONE_WAITING - deployment row is unlocked and deployment is properly set - yay!
    // - RETRY_MIGRATE - deployment row is unlocked but deployment is not yet properly set
    private WaitState tryUnlock(String deploymentUid) {
        try (Connection connection = dataSource.getConnection()) {
            startReadOnlyTransaction(connection);
            try {
                DeploymentRow row = getDeploymentRow(connection);
                if (row == null) {
                    // This happens if we have manually clear the deployment lock by deleting the row
                    return WaitState.RETRY_MIGRATE;
                }
                if (row.getLockingPodName() == null) {
                    logger.info("Deployment unlocked - continuing");
                    if (StringUtils.equals(row.getId(), deploymentUid)) {
                        logger.info("Deployment properly set - continuing");
                        return WaitState.DONE_WAITING;
                    }
                    // Other pod failed to migrate. Try the migrate again.
                    return WaitState.RETRY_MIGRATE;
                }
            } finally {
                commitReadOnlyTransaction(connection);
            }
        } catch (SQLException ex) {
            throw new MigrateException("Failed to connect to database", ex);
        }
        return WaitState.KEEP_WAITING; // deployment is still locked
    }

    private void releaseDeploymentLock() {
        final String updateTemplate = "UPDATE migrate.deployment_v1 SET locking_pod_name = NULL" +
            " WHERE dep_version = 1 AND locking_pod_name = '<podname>'";
        String updateSql = new ST(updateTemplate).add("podname", kubeService.getPodName()).render();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(updateSql)) {
            startTransaction(connection);
            try {
                DeploymentRow row = getDeploymentRow(connection);
                if ((row.getLockingPodName() == null) && (migrateConfiguration.getDropAllOnStart())) {
                    // Application of the migration released the lock for us. Nothing for us to do.
                    return;
                }

                int rows = statement.executeUpdate();
                commitTransaction(connection);
                if (rows != 1) {
                    throw new MigrateException("Failed to update and release deployment lock");
                }
            } finally {
                rollbackTransaction(connection);
            }
        } catch (SQLException ex) {
            throw new MigrateException("Update deployment failed", ex);
        }
    }

    public MigrateAction checkDeploymentState(String deploymentUid) {
        final String lockTableSql = "LOCK TABLE migrate.deployment_v1 IN EXCLUSIVE MODE";
        final String upsertTemplate = "INSERT INTO migrate.deployment_v1(dep_version, id, locking_pod_name)" +
            " VALUES (1, '<id>', '<podname>')" +
            " ON CONFLICT ON CONSTRAINT deployment_v1_pkey" +
            " DO UPDATE SET id = '<id>', locking_pod_name = '<podname>'";

        String upsertSql = new ST(upsertTemplate)
            .add("podname", kubeService.getPodName())
            .add("id", deploymentUid)
            .render();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement lockStatement = connection.prepareStatement(lockTableSql);
             PreparedStatement statement = connection.prepareStatement(upsertSql)) {

            startTransaction(connection);
            try {
                lockStatement.execute();

                DeploymentRow row = getDeploymentRow(connection);
                if (row != null) {
                    if (StringUtils.equals(row.getId(), deploymentUid)) {
                        if (row.getLockingPodName() == null) {
                            // The deployment uid matches and no one has the row locked. We are good to go.
                            return MigrateAction.NOTHING;
                        } else {
                            // The deployment uid matches, but the row is locked. We need to wait.
                            return MigrateAction.WAIT_FOR_UNLOCK;
                        }
                    }
                }

                // Either the row doesn't exist, or the uid is wrong
                int rows = statement.executeUpdate();
                if (rows != 1) {
                    throw new
                        MigrateException("Failed to upsert and take the deployment lock; that should be impossible");
                }
                return MigrateAction.MIGRATE;
            } finally {
                commitTransaction(connection);
            }
        } catch (SQLException ex) {
            throw new MigrateException("Update deployment failed", ex);
        }
    }

    // SQL State for table does not exist
    private static final String PSQL_TABLE_NOT_EXIST = "42P01";

    private DeploymentRow getDeploymentRow(Connection connection) {
        final String readSql = "SELECT id, locking_pod_name FROM migrate.deployment_v1 WHERE dep_version = 1";
        DeploymentRow row = null;

        try (PreparedStatement statement = connection.prepareStatement(readSql);
             ResultSet rs = statement.executeQuery()) {

            if (rs.next()) {
                row = new DeploymentRow()
                    .id(rs.getString("id"))
                    .lockingPodName(rs.getString("locking_pod_name"));
            }
        } catch (PSQLException ex) {
            if (!StringUtils.equals(ex.getSQLState(), PSQL_TABLE_NOT_EXIST)) {
                throw new MigrateException("Select deployment failed", ex);
            }
        } catch (SQLException ex) {
            throw new MigrateException("Select deployment failed", ex);
        }

        return row;
    }

    private void makeDeploymentTable() {
        final String schemaCreate = "CREATE SCHEMA IF NOT EXISTS migrate";
        final String tableCreate = "CREATE TABLE IF NOT EXISTS migrate.deployment_v1 (" +
            " dep_version integer primary key," +
            " id text," +
            " locking_pod_name text)";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement schemaStatement = connection.prepareStatement(schemaCreate);
             PreparedStatement tableStatement = connection.prepareStatement(tableCreate)) {

            startTransaction(connection);
            schemaStatement.execute();
            tableStatement.execute();
            commitTransaction(connection);

        } catch (SQLException ex) {
            throw new MigrateException("Table create failed", ex);
        }
    }

    private void migrateDatabase(String changesetFile, DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(changesetFile,
                new ClassLoaderResourceAccessor(),
                new JdbcConnection(connection));
            DatabaseChangeLogLock[] locks = liquibase.listLocks();
            for (DatabaseChangeLogLock lock : locks) {
                logger.info(String.format("dbChangeLogLock changeSet: %s, id: %s, lockedBy: %s, granted: %s",
                    changesetFile, lock.getId(), lock.getLockedBy(), lock.getLockGranted()));

                /*
                 * We can get into this state where one of the APIs is running migrations and gets shut down so that
                 * another API container can run. It will result in a lock that doesn't get released. This is similar
                 * to the problems we will have from deploying multiple containers at once that try to run migrations.
                 */
                logger.warn("forcing lock release");
                liquibase.forceReleaseLocks();
            }
            logger.info(String.format("dropAllOnStart is set to %s", migrateConfiguration.getDropAllOnStart()));
            if (migrateConfiguration.getDropAllOnStart()) {
                liquibase.dropAll(); // drops everything in the default schema. The migrate schema should be OK
            }
            logger.info(String.format("updateAllOnStart is set to %s", migrateConfiguration.getUpdateAllOnStart()));
            if (migrateConfiguration.getUpdateAllOnStart()) {
                liquibase.update(new Contexts()); // Run all migrations - no context filtering
            }
        } catch (LiquibaseException | SQLException ex) {
            throw new MigrateException("Failed to migrate database from " + changesetFile, ex);
        }
    }

    private void startTransaction(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        connection.setReadOnly(false);
    }

    private void commitTransaction(Connection connection) throws SQLException {
        connection.commit();
    }

    private void rollbackTransaction(Connection connection) throws SQLException {
        connection.rollback();
    }

    private void startReadOnlyTransaction(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setReadOnly(true);
    }

    private void commitReadOnlyTransaction(Connection connection) throws SQLException {
        connection.commit();
        connection.setReadOnly(false);
    }

}
