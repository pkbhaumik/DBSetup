package pb.ticket.service.dbsetup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class TicketServiceDB {
	private static final Logger LOGGER = Logger.getLogger(TicketServiceDB.class);
	private final String connectionString;

	public TicketServiceDB(String databaseServer, String databaseName, String userName, String password) {
		connectionString = String.format("jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s", databaseServer,
				databaseName, userName, password);
	}

	public void populateDatabase() {

	}

	private static Connection getSQLServerConnection(String sqlserverConenctionString) {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			LOGGER.error(e.getMessage());
			return null;
		}

		Connection sqlConnection = null;
		try {
			sqlConnection = DriverManager.getConnection(sqlserverConenctionString);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
		}

		return sqlConnection;
	}

}
