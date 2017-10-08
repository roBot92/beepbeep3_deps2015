package beepbeep3_deps2015_projekt;

import java.util.Collections;
import java.util.List;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.TaxiLog;
import onlab.event.Tick;
import onlab.utility.DataFileParser;

public class FileParserProcessor extends SingleProcessor {

	private DataFileParser parser;

	private List<TaxiLog> lastParsedTaxiLogs;

	public FileParserProcessor(int in_arity, int out_arity, DataFileParser parser) {
		super(in_arity, out_arity);
		this.parser = parser;
		if (parser != null) {
			lastParsedTaxiLogs = parser.parseNextLinesFromCSVGroupedByDropoffDate();
		}

	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> queue) throws ProcessorException {
		 //System.out.println("FilePArserProcessor starts. This is " +
		// Thread.currentThread() + " thread.");
		if (parser == null) {
			return false;
		}
		if (input[0] instanceof Tick) {
			Tick inputTick = (Tick) input[0];
			long currentParsedTime = getCurrentParsedTime();

			// if the next taxiLogs hasnt came yet. For real time, this part would be
			// slightly different. For now, we pass an empty list.
			if (inputTick.getCurrentTime() < currentParsedTime) {
				queue.add(new Object[] { Collections.emptyList(), inputTick });
				return true;
			}

			// If the current parsed taxilogs dropoff datetime equals to the time of tick,
			// we can pass them into the tube
			if (inputTick.getCurrentTime() == currentParsedTime) {
				if (!parser.hasNextLine()) {

					parser.close();
					return false;
				}
				queue.add(new Object[] { lastParsedTaxiLogs, inputTick });
				lastParsedTaxiLogs = parser.parseNextLinesFromCSVGroupedByDropoffDate();
				System.out.println();
				return true;
			}

			// TODO what if currentTime is after dropoff times.
			
		}
		return false;

	}

	@Override
	public Processor clone() {
		try {
			return new FileParserProcessor(this.getInputArity(), this.getOutputArity(),
					(DataFileParser) parser.clone());
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}

	public void closeDataFileParser() {
		if (parser != null) {
			parser.close();
		}
	}

	public long getCurrentParsedTime() {
		if (lastParsedTaxiLogs.isEmpty()) {
			return -1;
		}
		return lastParsedTaxiLogs.get(0).getDropoff_datetime().getTime();
	}
	

}