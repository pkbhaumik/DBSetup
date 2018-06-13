package pb.ticket.service.dbsetup;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TicketServiceDB {
	private final String connectionString;

	public TicketServiceDB(String databaseServer, String databaseName, String userName, String password) {
		connectionString = String.format("jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s;Max Pool Size=50",
				databaseServer, databaseName, userName, password);
	}

	public void populateDatabase() {
		System.out.println("Verifying Travel Service Database.");

		try (Connection sqlConnection = getSQLServerConnection(this.connectionString)) {

			List<Stage> stageInfo = new ArrayList<Stage>();

			try (Statement sqlStmt = sqlConnection.createStatement()) {
				int recordCount = 0;
				try (ResultSet rs = sqlStmt.executeQuery("SELECT COUNT(*) FROM [TS].[SeatMap]")) {

					if (rs != null && rs.next()) {
						recordCount = rs.getInt(1);
					}
				}

				if (recordCount == 0) {
					System.out.println("SeatMap table is empty. Populating [SeatMap] table.");

					// Get the stage information
					try (ResultSet rs = sqlStmt
							.executeQuery("SELECT LevelId, TotalTows, SeatsInRow FROM [TS].[Stage] ORDER BY LevelId")) {

						if (rs != null) {
							while (rs.next()) {
								stageInfo.add(new Stage(rs.getInt(1), rs.getInt(2), rs.getInt(3)));
							}
						}
					}
				}
			}

			String insertQuery = "INSERT INTO [TS].[SeatMap] (LevelId, RowNumber, SeatNumber) VALUES(?,?,?)";
			if (stageInfo.size() > 0) {
				try {
					sqlConnection.setAutoCommit(false);
					for (Stage s : stageInfo) {
						try (PreparedStatement prepStmt = sqlConnection.prepareStatement(insertQuery)) {
							for (int row = 1; row < s.getTotalRows(); row++) {
								for (int seatNumber = 1; seatNumber < s.getSeatsInRow(); seatNumber++) {

									prepStmt.setInt(1, s.getLevelId());
									prepStmt.setInt(2, row);
									prepStmt.setInt(3, seatNumber);
									prepStmt.addBatch();
								}
							}

							prepStmt.executeBatch();
							sqlConnection.commit();
						}
					}
				} catch (Exception ex) {
					sqlConnection.rollback();
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Done with Database setup.");

	}

	private static Connection getSQLServerConnection(String sqlserverConenctionString) {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}

		Connection sqlConnection = null;
		try {
			sqlConnection = DriverManager.getConnection(sqlserverConenctionString);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return sqlConnection;
	}

	public void releaseHold(Integer key) {
		try (Connection sqlConnection = getSQLServerConnection(this.connectionString)) {
			try (CallableStatement stmt = sqlConnection.prepareCall("{call ReleaseHold(?)}")) {
				stmt.setInt(1, key);

				stmt.executeUpdate();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
