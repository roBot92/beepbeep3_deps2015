package beepbeep3_deps2015_projekt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
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
import onlab.event.Tick;
import onlab.main.DebsMain;
import onlab.positioning.CellHelper;
import onlab.utility.DataFileParser;
import onlab.utility.FrequentRoutesToplistSet;
import onlab.utility.ProfitableAreaToplistSet;

public class BeepApp {

	private static FrequentRoutesToplistSet freqRouteToplist = new FrequentRoutesToplistSet();
	private static ProfitableAreaToplistSet profAreaToplist = new ProfitableAreaToplistSet();

	public static final long TEST_INTERVAL_IN_IN_MS = 24 * 60 * 60 * 1000;
	public static final long BENCHMARK_FREQUENCY_IN_MS = 1000 * 60;

	public static final int TASK_NUMBER_ONE = 1;
	public static final int TASK_NUMBER_TWO = 2;

	public static void main(String[] args) {

		try {
			runTask1();
			runTask2();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		

	}

	public static DataFileParser initializeDataFileParser(CellHelper chelper) {

		try {
			return new DataFileParser(DebsMain.DATA_FILE_URL, DebsMain.DELIMITER, DebsMain.columncount, chelper);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}

	}

	public static void runTask(String resultFileName, int runningMode, int taskNumber)
			throws ConnectorException, IOException {

		BufferedWriter resultFileWriter = null;
		try {
			resultFileWriter = initializeResultFileWriter(resultFileName);
			Pushable pushable = null;
			if (taskNumber == TASK_NUMBER_ONE) {
				pushable = initializeTask1(runningMode, resultFileWriter);
			} else if (taskNumber == TASK_NUMBER_TWO) {
				pushable = initializeTask2(runningMode, resultFileWriter);
			}
			FileParserProcessor fPProc = (FileParserProcessor) pushable.getProcessor();
			long startingTime = fPProc.getCurrentParsedTime();

			for (long i = startingTime; i <= TEST_INTERVAL_IN_IN_MS + startingTime; i += 1000) {
				pushable.push(new Tick(i));
			}
			fPProc.closeDataFileParser();

		} catch (IOException e) {
			e.printStackTrace();
			return;
		} finally {
			if (resultFileWriter != null) {
				resultFileWriter.close();
			}
		}

	}

	public static void runTask1() throws ConnectorException, IOException {

		runTask(DebsMain.task1ResultToCompareFileName,DebsMain.OUTPUT_COOMPARING_MODE, TASK_NUMBER_ONE);
		freqRouteToplist.clear();
		runTask(DebsMain.task1TimeMeasuringResultFileName,DebsMain.TIME_MEASURING_MODE, TASK_NUMBER_ONE);
		freqRouteToplist.clear();
		runTask(DebsMain.task1MemoryMeasuringResultFileName,DebsMain.MEMORY_MEASURING_MODE, TASK_NUMBER_ONE);
	
	}
	
	public static void runTask2() throws ConnectorException, IOException {

		
		runTask(DebsMain.task2ResultToCompareFileName,DebsMain.OUTPUT_COOMPARING_MODE, TASK_NUMBER_TWO);
		profAreaToplist.clear();
		runTask(DebsMain.task2TimeMeasuringResultFileName,DebsMain.TIME_MEASURING_MODE, TASK_NUMBER_TWO);
		profAreaToplist.clear();
		runTask(DebsMain.task2MemoryMeasuringResultFileName,DebsMain.MEMORY_MEASURING_MODE, TASK_NUMBER_TWO);
	
	}
	
	




	public static Pushable initializeTask1(int runningMode, BufferedWriter resultFileWriter) {
		CellHelper chelper = new CellHelper(DebsMain.FIRST_CELL_X, DebsMain.FIRST_CELL_Y, DebsMain.SHIFT_X,
				DebsMain.SHIFT_Y, 300);

		FileParserProcessor fProc = new FileParserProcessor(1, 2, initializeDataFileParser(chelper));
		InvalidTaxiLogFilterProcessor filterProc = new InvalidTaxiLogFilterProcessor(1, 1, tlog -> {
			return tlog.getPickup_datetime() == null || tlog.getDropoff_datetime() == null
					|| tlog.getPickup_cell() == null || tlog.getDropoff_cell() == null;
		});
		NewRouteComputingProcessor nrProc = new NewRouteComputingProcessor(1, 1, freqRouteToplist);
		ExpiringRoutesComputingProcessor exProc = new ExpiringRoutesComputingProcessor(2, 1, freqRouteToplist);
		FilePrinterProcessor fPrintProc = new FilePrinterProcessor(1, 0, freqRouteToplist, runningMode,
				resultFileWriter, BENCHMARK_FREQUENCY_IN_MS, fProc);

		/*
		 * 0 0 0 0 0 0 fProc --->filterProc--->nrProc ---> orProc |1 ^1
		 * |__________________________________|
		 */
		Connector.connect(fProc, 0, filterProc, 0);
		Connector.connect(filterProc, 0, nrProc, 0);
		Connector.connect(nrProc, 0, exProc, 0);
		Connector.connect(fProc, 1, exProc, 1);
		Connector.connect(exProc, 0, fPrintProc, 0);

		return fProc.getPushableInput(0);
	}

	public static Pushable initializeTask2(int runningMode, BufferedWriter resultFileWriter) {
		CellHelper chelper = new CellHelper(DebsMain.FIRST_CELL_X, DebsMain.FIRST_CELL_Y,
				DebsMain.SHIFT_X.divide(BigDecimal.valueOf(2)), DebsMain.SHIFT_Y.divide(BigDecimal.valueOf(2)), 600);
		FileParserProcessor fProc = new FileParserProcessor(1, 2, initializeDataFileParser(chelper));
		InvalidTaxiLogFilterProcessor filterProc = new InvalidTaxiLogFilterProcessor(1, 1, tlog -> {
			return tlog.getPickup_datetime() == null || tlog.getDropoff_datetime() == null
					|| tlog.getPickup_cell() == null || tlog.getDropoff_cell() == null || tlog.getFare_amount() == null
					|| tlog.getFare_amount().compareTo(BigDecimal.ZERO) < 1 || tlog.getTip_amount() == null
					|| tlog.getHack_license() == null;
		});

		MedianComputingProcessor medProc = new MedianComputingProcessor(2, 2, profAreaToplist);
		TaxiCountComputingProcessor countProc = new TaxiCountComputingProcessor(2, 1, profAreaToplist);
		FilePrinterProcessor fPrintProc = new FilePrinterProcessor(1, 0, profAreaToplist, runningMode, resultFileWriter,
				BENCHMARK_FREQUENCY_IN_MS, fProc);
		;

		Connector.connect(fProc, 0, filterProc, 0);
		Connector.connect(filterProc, 0, medProc, 0);
		Connector.connect(fProc, 1, medProc, 1);
		Connector.connect(medProc, 0, countProc, 0);
		Connector.connect(medProc, 1, countProc, 1);
		Connector.connect(countProc, 0, fPrintProc, 0);

		Pushable fprocPushable = fProc.getPushableInput(0);

		return fprocPushable;
	}

	public static BufferedWriter initializeResultFileWriter(String fileName) throws IOException {
		File resultFile = new File(fileName);
		if (resultFile.exists()) {
			resultFile.delete();
		}
		return new BufferedWriter(new FileWriter(resultFile));

	}
}
