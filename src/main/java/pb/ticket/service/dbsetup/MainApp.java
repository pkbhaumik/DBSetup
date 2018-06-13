package pb.ticket.service.dbsetup;

/**
 * @author Pralay Bhaumik
 * 
 * Ticket Service Database service to manage seat holds expiration.
 * Listen to Kafka queue to receive ticket hold and reservation information.
 */
public class MainApp {
	private static TicketHoldMonitor monitor;

	public static void main(String[] args) {
		if (args.length != 5) {
			System.out.println("Invalid number of argiments");
			System.out.println(
					"Usage: {java -jar pb-ticket-db-service-jar-with-dependencies.jar <Database_server> <Database_name> <User_name> <Password> <KAFKA_BROKERS>}");
			return;
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				if (MainApp.monitor != null) {
					MainApp.monitor.exit();
				}
			}
		});

		TicketServiceDB tsDB = new TicketServiceDB(args[0], args[1], args[2], args[3]);
		tsDB.populateDatabase();

		try {
			MainApp.monitor = new TicketHoldMonitor(args[4], tsDB);
			Thread monitorThread = new Thread(MainApp.monitor);

			monitorThread.setName("Ticket hold release thread.");
			monitorThread.start();

			monitorThread.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
