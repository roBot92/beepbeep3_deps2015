package beepbeep3_deps2015_projekt;


import java.io.FileNotFoundException;
import java.math.BigDecimal;



import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.Connector.ConnectorException;
import ca.uqac.lif.cep.Pushable;
import ca.uqac.lif.cep.interpreter.Interpreter.ParseException;
import onlab.event.Route;
import onlab.event.Tick;
import onlab.main.DebsMain;
import onlab.positioning.CellHelper;
import onlab.utility.DataFileParser;
import onlab.utility.FrequentRoutesToplistSet;

public class BeepApp {

	// public static String FILENAME =
	// "C:\\Users\\Boti\\workspace\\beepbeep3_deps2015_projekt\\src\\main\\resources\\testcsv.csv";
	public static String FILENAME = "testcsv.csv";
	private static FrequentRoutesToplistSet<Route> freqRouteToplist = new FrequentRoutesToplistSet<Route>();

	public static void main(String[] args) throws ParseException, FileNotFoundException, ConnectorException {

		FileParserProcessor fproc = new FileParserProcessor(1, 1, initializeDataFileParser());
		ComputingProcessor cproc = new ComputingProcessor(1, 1);
		Connector.connect(fproc, cproc);

		Pushable fprocPushable = fproc.getPushableInput(0);
		
		Tick tick = new Tick(-1);
		for(int i = 0; i < 100 ; i++) {
			fprocPushable.push(tick);
		}
		
	}

	public static DataFileParser initializeDataFileParser() {
		CellHelper chelper = new CellHelper(DebsMain.FIRST_CELL_X, DebsMain.FIRST_CELL_Y,
				DebsMain.SHIFT_X.divide(BigDecimal.valueOf(2)), DebsMain.SHIFT_Y.divide(BigDecimal.valueOf(2)), 600);

		try {
			return new DataFileParser(DebsMain.DATA_FILE_URL, DebsMain.DELIMITER, DebsMain.columncount, chelper);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}

	}

}
