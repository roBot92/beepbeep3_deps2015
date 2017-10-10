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

		FileParserProcessor fProc = new FileParserProcessor(1, 2, initializeDataFileParser());
		InvalidTaxiLogFilter filterProc = new InvalidTaxiLogFilter(1, 1);
		NewRouteComputingProcessor nrProc = new NewRouteComputingProcessor(1, 1, freqRouteToplist);
		OutrunningRoutesComputingProcessor orProc = new OutrunningRoutesComputingProcessor(2, 1, freqRouteToplist);

		/*       0   0         0  0      0     0
		 * fProc --->filterProc--->nrProc ---> orProc 
		 * |1                                 ^1
		 * |__________________________________|
		 */
		Connector.connect(fProc, 0, filterProc, 0);
		Connector.connect(filterProc, 0, nrProc, 0);
		Connector.connect(nrProc, 0, orProc, 0);
		Connector.connect(fProc, 1, orProc, 1);

		Pushable fprocPushable = fProc.getPushableInput(0);

		long time = fProc.getCurrentParsedTime();
		// going for 1 hour
		long oneDayInMinutes = 24 * 60 * 60;
		for (long i = 0; i < oneDayInMinutes; i++) {
			fprocPushable.push(new Tick(time + i * 1000));

		}

		fProc.closeDataFileParser();

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
