package hu.bme.mit.beepbeep3.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.Connector.ConnectorException;
import hu.bme.mit.beepbeep3.processors.FileParserProcessor;
import hu.bme.mit.beepbeep3.processors.FilePrinterProcessor;
import hu.bme.mit.beepbeep3.processors.InvalidTaxiLogFilterProcessor;
import hu.bme.mit.beepbeep3.processors.task1.ExpiringRoutesComputingProcessor;
import hu.bme.mit.beepbeep3.processors.task1.NewRouteComputingProcessor;
import hu.bme.mit.beepbeep3.processors.task2.MedianComputingProcessor;
import hu.bme.mit.beepbeep3.processors.task2.TaxiCountComputingProcessor;
import hu.bme.mit.entities.Tick;
import hu.bme.mit.positioning.CellHelper;
import hu.bme.mit.toplist.FrequentRoutesToplistSet;
import hu.bme.mit.toplist.ProfitableAreaToplistSet;
import hu.bme.mit.utility.DataFileParser;
import hu.bme.mit.utility.ExecutionSetup;
import ca.uqac.lif.cep.Pushable;

public class BeepApp {

	private static FrequentRoutesToplistSet freqRouteToplist = new FrequentRoutesToplistSet();
	private static ProfitableAreaToplistSet profAreaToplist = new ProfitableAreaToplistSet();

	public static final long TEST_INTERVAL_IN_IN_MS = 31 * 24 * 60 * 60 * 1000l;
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
			return new DataFileParser(ExecutionSetup.DATA_FILE_URL, ExecutionSetup.DELIMITER, ExecutionSetup.columncount, chelper);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}

	}

	public static void runTask(String resultFileName, int runningMode, int taskNumber)
			throws ConnectorException, IOException {

		BufferedWriter resultFileWriter = null;
		FileParserProcessor fPProc = null;
		try {
			resultFileWriter = initializeResultFileWriter(resultFileName);
			Pushable pushable = null;
			if (taskNumber == TASK_NUMBER_ONE) {
				pushable = initializeTask1(runningMode, resultFileWriter);
			} else if (taskNumber == TASK_NUMBER_TWO) {
				pushable = initializeTask2(runningMode, resultFileWriter);
			}
			fPProc = (FileParserProcessor) pushable.getProcessor();
			long startingTime = fPProc.getCurrentParsedTime();

			System.gc();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			for (long i = startingTime; i <= TEST_INTERVAL_IN_IN_MS + startingTime; i += 1000) {
				pushable.push(new Tick(i));
			}
			fPProc.closeDataFileParser();
			if(runningMode == ExecutionSetup.TIME_MEASURING_MODE){
				resultFileWriter.newLine();
				resultFileWriter.write("Ending time: " + DataFileParser.SIMPLE_DATE_FORMAT.format(new Date(System.currentTimeMillis())));
			}

		} catch (IOException e) {
			e.printStackTrace();
			return;
		} finally {
			if (resultFileWriter != null) {
				resultFileWriter.close();
			}
			if(fPProc != null){
				fPProc.closeDataFileParser();
			}
		}

	}

	public static void runTask1() throws ConnectorException, IOException {

		runTask(ExecutionSetup.task1ResultToCompareFileName,ExecutionSetup.OUTPUT_COOMPARING_MODE, TASK_NUMBER_ONE);
		freqRouteToplist.clear();
		runTask(ExecutionSetup.task1TimeMeasuringResultFileName,ExecutionSetup.TIME_MEASURING_MODE, TASK_NUMBER_ONE);
		freqRouteToplist.clear();
		runTask(ExecutionSetup.task1MemoryMeasuringResultFileName,ExecutionSetup.MEMORY_MEASURING_MODE, TASK_NUMBER_ONE);
	
	}
	
	public static void runTask2() throws ConnectorException, IOException {

		
		
	//	runTask(ExecutionSetup.task2MemoryMeasuringResultFileName,ExecutionSetup.MEMORY_MEASURING_MODE, TASK_NUMBER_TWO);
	//	profAreaToplist.clear();
	//	runTask(ExecutionSetup.task2ResultToCompareFileName,ExecutionSetup.OUTPUT_COOMPARING_MODE, TASK_NUMBER_TWO);
	//	profAreaToplist.clear();
		runTask(ExecutionSetup.task2TimeMeasuringResultFileName,ExecutionSetup.TIME_MEASURING_MODE, TASK_NUMBER_TWO);
		
	
	}
	
	




	public static Pushable initializeTask1(int runningMode, BufferedWriter resultFileWriter) {
		CellHelper chelper = new CellHelper(ExecutionSetup.FIRST_CELL_X, ExecutionSetup.FIRST_CELL_Y, ExecutionSetup.SHIFT_X,
				ExecutionSetup.SHIFT_Y, 300);

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
		CellHelper chelper = new CellHelper(ExecutionSetup.FIRST_CELL_X, ExecutionSetup.FIRST_CELL_Y,
				ExecutionSetup.SHIFT_X.divide(BigDecimal.valueOf(2)), ExecutionSetup.SHIFT_Y.divide(BigDecimal.valueOf(2)), 600);
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
