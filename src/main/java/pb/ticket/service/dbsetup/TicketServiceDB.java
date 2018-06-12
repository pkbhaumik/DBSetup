package pb.ticket.service.dbsetup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class TicketServiceDB {
	private static final Logger LOGGER = Logger.getLogger(TicketServiceDB.class);
	private final String connectionString;

	public TicketServiceDB(String databaseServer, String databaseName, String userName, String password) {
		connectionString = String.format("jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s", databaseServer,
				databaseName, userName, password);
	}

	public void populateDatabase() {
		System.out.println("Verifying Travel Service Database.");

		try (Connection sqlConnection = getSQLServerConnection(this.connectionString)) {
			
			try (Statement sqlStmt = sqlConnection.createStatement()) {
				int recordCount = 0;
				try (ResultSet rs = sqlStmt.executeQuery("SELECT COUNT(*) FROM [TS].[SeatMap]")) {

					if (rs != null && rs.next()) {
						recordCount = rs.getInt(1);
					}
				}
				
				if (recordCount == 0) {
					System.out.println("SeatMap table is empty. Population the database.");
					
					//Get the stage information
					try (ResultSet rs = sqlStmt.executeQuery("SELECT LevelId, TotalTows, SeatsInRow FROM [TS].[Stage]")) {
						
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

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
