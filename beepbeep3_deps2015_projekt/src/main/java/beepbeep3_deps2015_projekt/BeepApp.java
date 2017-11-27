package beepbeep3_deps2015_projekt;

import java.io.FileNotFoundException;
import java.math.BigDecimal;

import beepbeep3_deps2015_projekt.processors.FileParserProcessor;
import beepbeep3_deps2015_projekt.processors.FilePrinterProcessor;
import beepbeep3_deps2015_projekt.processors.InvalidTaxiLogFilterProcessor;
import beepbeep3_deps2015_projekt.processors.task1.NewRouteComputingProcessor;
import beepbeep3_deps2015_projekt.processors.task2.MedianComputingProcessor;
import beepbeep3_deps2015_projekt.processors.task2.TaxiCountComputingProcessor;
import beepbeep3_deps2015_projekt.processors.task1.ExpiringRoutesComputingProcessor;
import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.Connector.ConnectorException;
import ca.uqac.lif.cep.Pushable;
import ca.uqac.lif.cep.interpreter.Interpreter.ParseException;
import onlab.event.Tick;
import onlab.main.DebsMain;
import onlab.positioning.CellHelper;
import onlab.utility.DataFileParser;
import onlab.utility.FrequentRoutesToplistSet;
import onlab.utility.ProfitableAreaToplistSet;

public class BeepApp {

	// public static String FILENAME =
	// "C:\\Users\\Boti\\workspace\\beepbeep3_deps2015_projekt\\src\\main\\resources\\testcsv.csv";
	public static String FILENAME = "testcsv.csv";
	private static FrequentRoutesToplistSet freqRouteToplist = new FrequentRoutesToplistSet();
	private static ProfitableAreaToplistSet profAreaToplist = new ProfitableAreaToplistSet();
	private static String RESULT_FILE_PATH = "C:\\Users\\Boti\\git\\beepbeep3_deps2015\\beepbeep3_deps2015_projekt\\result.txt";
	private static long printFrequencyInMillisec = 60 * 60 * 1000;

	private static long TEST_INTERVAL_IN_IN_MS = 1 * 60 * 60 * 1000;

	public static void main(String[] args) throws ParseException, FileNotFoundException, ConnectorException {

		// runTask1();
		runTask2();

	}

	public static DataFileParser initializeDataFileParser(CellHelper chelper) {

		try {
			return new DataFileParser(DebsMain.DATA_FILE_URL, DebsMain.DELIMITER, DebsMain.columncount, chelper);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}

	}

	public static void runTask1() throws ConnectorException {
		CellHelper chelper = new CellHelper(DebsMain.FIRST_CELL_X, DebsMain.FIRST_CELL_Y,
				DebsMain.SHIFT_X.divide(BigDecimal.valueOf(2)), DebsMain.SHIFT_Y.divide(BigDecimal.valueOf(2)), 600);
		FileParserProcessor fProc = new FileParserProcessor(1, 2, initializeDataFileParser(chelper));
		InvalidTaxiLogFilterProcessor filterProc = new InvalidTaxiLogFilterProcessor(1, 1, tlog -> {
			return tlog.getPickup_datetime() == null || tlog.getDropoff_datetime() == null
					|| tlog.getPickup_cell() == null || tlog.getDropoff_cell() == null;
		});
		NewRouteComputingProcessor nrProc = new NewRouteComputingProcessor(1, 1, freqRouteToplist);
		ExpiringRoutesComputingProcessor exProc = new ExpiringRoutesComputingProcessor(2, 1, freqRouteToplist);
		FilePrinterProcessor fPrintProc = new FilePrinterProcessor(1, 0, RESULT_FILE_PATH, freqRouteToplist,
				printFrequencyInMillisec);

		/*
		 * 0 0 0 0 0 0 fProc --->filterProc--->nrProc ---> orProc |1 ^1
		 * |__________________________________|
		 */
		Connector.connect(fProc, 0, filterProc, 0);
		Connector.connect(filterProc, 0, nrProc, 0);
		Connector.connect(nrProc, 0, exProc, 0);
		Connector.connect(fProc, 1, exProc, 1);
		Connector.connect(exProc, 0, fPrintProc, 0);

		Pushable fprocPushable = fProc.getPushableInput(0);

		long startingTime = fProc.getCurrentParsedTime();

		for (long i = startingTime; i < TEST_INTERVAL_IN_IN_MS + startingTime; i += 1000) {
			fprocPushable.push(new Tick(i));
		}

		fProc.closeDataFileParser();

	}

	public static void runTask2() throws ConnectorException {
		CellHelper chelper = new CellHelper(DebsMain.FIRST_CELL_X, DebsMain.FIRST_CELL_Y, DebsMain.SHIFT_X,
				DebsMain.SHIFT_Y, 300);
		FileParserProcessor fProc = new FileParserProcessor(1, 2, initializeDataFileParser(chelper));
		InvalidTaxiLogFilterProcessor filterProc = new InvalidTaxiLogFilterProcessor(1, 1, tlog -> {
			return tlog.getPickup_datetime() == null || tlog.getDropoff_datetime() == null
					|| tlog.getPickup_cell() == null || tlog.getDropoff_cell() == null || tlog.getFare_amount() == null
					|| tlog.getFare_amount().compareTo(BigDecimal.ZERO) < 1 || tlog.getTip_amount() == null
					|| tlog.getHack_license() == null;
		});

		MedianComputingProcessor medProc = new MedianComputingProcessor(2, 2, profAreaToplist);
		TaxiCountComputingProcessor countProc = new TaxiCountComputingProcessor(2, 1, profAreaToplist);
		FilePrinterProcessor fPrintProc = new FilePrinterProcessor(1, 0, RESULT_FILE_PATH, profAreaToplist,
				printFrequencyInMillisec);

		Connector.connect(fProc, 0, filterProc, 0);
		Connector.connect(filterProc, 0, medProc, 0);
		Connector.connect(fProc, 1, medProc, 1);
		Connector.connect(medProc, 0, countProc, 0);
		Connector.connect(medProc, 1, countProc, 1);
		Connector.connect(countProc, 0, fPrintProc, 0);

		Pushable fprocPushable = fProc.getPushableInput(0);

		long startingTime = fProc.getCurrentParsedTime();

		for (long i = startingTime; i <= TEST_INTERVAL_IN_IN_MS + startingTime; i += 1000) {
			fprocPushable.push(new Tick(i));
		}

		fProc.closeDataFileParser();

	}
}
