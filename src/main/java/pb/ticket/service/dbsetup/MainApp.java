package pb.ticket.service.dbsetup;

/**
 * Hello world!
 *
 */
public class MainApp 
{
    public static void main( String[] args )
    {
    	if (args.length != 4) {
    		System.out.println("Invalid number of argiments");
    		System.out.println(
					"Usage: {jar dbsetup.jar  pb.ticket.service.dbsetup.MainApp <Database_server> <Database_name> <User_name> <Password>}");
    		return;
    	}
    	
    	TicketServiceDB tsDB = new TicketServiceDB(args[0], args[1], args[2], args[3]);
    	tsDB.populateDatabase();
    }
}
