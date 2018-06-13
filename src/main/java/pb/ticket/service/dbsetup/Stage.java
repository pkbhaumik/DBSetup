package pb.ticket.service.dbsetup;

public class Stage {
	
	private final int levelId;
	private final int totalRows;
	private final int seatsInRow;
	
	public Stage(int levelId, int totalRows, int seatsInRow) {
		this.levelId = levelId;
		this.totalRows = totalRows;
		this.seatsInRow = seatsInRow;
	}

	public int getLevelId() {
		return levelId;
	}

	public int getTotalRows() {
		return totalRows;
	}

	public int getSeatsInRow() {
		return seatsInRow;
	}
}
