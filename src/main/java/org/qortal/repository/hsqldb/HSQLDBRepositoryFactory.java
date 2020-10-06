package org.qortal.repository.hsqldb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hsqldb.HsqlException;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.HSQLDBPool;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryFactory;

public class HSQLDBRepositoryFactory implements RepositoryFactory {

	private static final Logger LOGGER = LogManager.getLogger(HSQLDBRepositoryFactory.class);
	private static final int POOL_SIZE = 100;

	private static final String blockchainFileUrlTemplate = "jdbc:hsqldb:file:%s" + File.separator + "blockchain;create=true;hsqldb.full_log_replay=true";
	private static final String blockchainMemoryUrl = "jdbc:hsqldb:mem:blockchain";

	private static final String nodeLocalFileUrlTemplate = "jdbc:hsqldb:file:%s" + File.separator + "node-local;create=true;hsqldb.full_log_replay=true";
	private static final String nodeLocalMemoryUrl = "jdbc:hsqldb:mem:node-local";

	/** Log getConnection() calls that take longer than this. (ms) */
	private static final long SLOW_CONNECTION_THRESHOLD = 1000L;

	private final String repositoryPath;

	private final String blockchainConnectionUrl;
	private final String nodeLocalConnectionUrl;

	private final HSQLDBPool blockchainConnectionPool;
	private final HSQLDBPool nodeLocalConnectionPool;

	/**
	 * Constructs new RepositoryFactory using passed repository path, or null for in-memory.
	 * 
	 * @param repositoryPath
	 * @throws DataException <i>without throwable</i> if repository in use by another process.
	 * @throws DataException <i>with throwable</i> if repository cannot be opened for some other reason.
	 */
	public HSQLDBRepositoryFactory(String repositoryPath) throws DataException {
		this.repositoryPath = repositoryPath;

		// one-time initialization goes in here
		if (repositoryPath != null) {
			this.blockchainConnectionUrl = String.format(blockchainFileUrlTemplate, repositoryPath);
			this.nodeLocalConnectionUrl = String.format(nodeLocalFileUrlTemplate, repositoryPath);
		} else {
			this.blockchainConnectionUrl = blockchainMemoryUrl;
			this.nodeLocalConnectionUrl = nodeLocalMemoryUrl;
		}

		// Check no-one else is accessing database
		try (Connection connection = DriverManager.getConnection(this.blockchainConnectionUrl)) {
			// We only need to check we can obtain connection. It will be auto-closed.
		} catch (SQLException e) {
			Throwable cause = e.getCause();
			if (!(cause instanceof HsqlException))
				throw new DataException("Unable to open repository: " + e.getMessage(), e);

			HsqlException he = (HsqlException) cause;
			if (he.getErrorCode() == -ErrorCode.LOCK_FILE_ACQUISITION_FAILURE)
				throw new DataException("Unable to lock repository: " + e.getMessage());

			if (he.getErrorCode() != -ErrorCode.ERROR_IN_LOG_FILE && he.getErrorCode() != -ErrorCode.M_DatabaseScriptReader_read)
				throw new DataException("Unable to read repository: " + e.getMessage(), e);

			// Attempt recovery?
			HSQLDBRepository.attemptRecovery(repositoryPath);
		}

		this.blockchainConnectionPool = new HSQLDBPool(POOL_SIZE);
		this.blockchainConnectionPool.setUrl(this.blockchainConnectionUrl);

		Properties blockchainProperties = new Properties();
		blockchainProperties.setProperty("close_result", "true"); // Auto-close old ResultSet if Statement creates new ResultSet
		this.blockchainConnectionPool.setProperties(blockchainProperties);

		this.nodeLocalConnectionPool = new HSQLDBPool(POOL_SIZE);
		this.nodeLocalConnectionPool.setUrl(this.nodeLocalConnectionUrl);

		Properties nodeLocalProperties = new Properties();
		nodeLocalProperties.setProperty("close_result", "true"); // Auto-close old ResultSet if Statement creates new ResultSet
		this.nodeLocalConnectionPool.setProperties(nodeLocalProperties);

		// Perform DB updates?
		try (final Connection blockchainConnection = this.blockchainConnectionPool.getConnection();
				final Connection nodeLocalConnection = this.nodeLocalConnectionPool.getConnection()) {
			HSQLDBDatabaseUpdates.updateDatabase(blockchainConnection);
		} catch (SQLException e) {
			throw new DataException("Repository initialization error", e);
		}
	}

	@Override
	public RepositoryFactory reopen() throws DataException {
		return new HSQLDBRepositoryFactory(this.repositoryPath);
	}

	@Override
	public Repository getRepository() throws DataException {
		try {
			return new HSQLDBRepository(this.getBlockchainConnection(), this.getNodeLocalConnection());
		} catch (SQLException e) {
			throw new DataException("Repository instantiation error", e);
		}
	}

	@Override
	public Repository tryRepository() throws DataException {
		try {
			Connection blockchainConnection = this.tryBlockchainConnection();
			if (blockchainConnection == null)
				return null;

			Connection nodeLocalConnection = this.tryNodeLocalConnection();
			if (nodeLocalConnection == null)
				return null;

			return new HSQLDBRepository(blockchainConnection, nodeLocalConnection);
		} catch (SQLException e) {
			throw new DataException("Repository instantiation error", e);
		}
	}

	private Connection getBlockchainConnection() throws SQLException {
		return getConnection(this.blockchainConnectionPool, "blockchain");
	}

	private Connection tryBlockchainConnection() throws SQLException {
		return tryConnection(this.blockchainConnectionPool);
	}

	private Connection getNodeLocalConnection() throws SQLException {
		return getConnection(this.nodeLocalConnectionPool, "node-local");
	}

	private Connection tryNodeLocalConnection() throws SQLException {
		return tryConnection(this.nodeLocalConnectionPool);
	}

	private Connection getConnection(HSQLDBPool pool, String poolName) throws SQLException {
		final long before = System.currentTimeMillis();
		Connection connection = pool.getConnection();
		final long delay = System.currentTimeMillis() - before;

		if (delay > SLOW_CONNECTION_THRESHOLD)
			// This could be an indication of excessive repository use, or insufficient pool size
			LOGGER.warn(() -> String.format("Fetching repository connection from %s pool took %dms (threshold: %dms)",
					poolName,
					delay,
					SLOW_CONNECTION_THRESHOLD));

		setupConnection(connection);
		return connection;
	}

	private Connection tryConnection(HSQLDBPool pool) throws SQLException {
		Connection connection = pool.tryConnection();
		if (connection == null)
			return null;

		setupConnection(connection);
		return connection;
	}

	private void setupConnection(Connection connection) throws SQLException {
		// Set transaction level
		connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		connection.setAutoCommit(false);
	}

	@Override
	public void close() throws DataException {
		try {
			// Close all existing connections immediately
			this.blockchainConnectionPool.close(0);
			this.nodeLocalConnectionPool.close(0);

			// Now that all connections are closed, create a dedicated connection to shut down repository
			try (Connection connection = DriverManager.getConnection(this.blockchainConnectionUrl);
					Statement stmt = connection.createStatement()) {
				stmt.execute("SHUTDOWN");
			}

			try (Connection connection = DriverManager.getConnection(this.nodeLocalConnectionUrl);
					Statement stmt = connection.createStatement()) {
				stmt.execute("SHUTDOWN");
			}
		} catch (SQLException e) {
			throw new DataException("Error during repository shutdown", e);
		}
	}

}
