package beepbeep3_deps2015_projekt.processors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.Tick;

public class FilePrinterProcessor extends SingleProcessor {

	private Object printable;
	private Path logFilePath;
	long printingFrequency;
	long lastTimePrinted = -1;

	public FilePrinterProcessor(int in_arity, int out_arity, String logFilePath, Object printable, long frequency) {
		super(in_arity, out_arity);
		this.logFilePath = Paths.get(logFilePath);
		this.printable = printable;
		printingFrequency = frequency;

	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if (input[0] instanceof Tick) {
			Tick tick = (Tick) input[0];
			if (lastTimePrinted == -1) {
				writeToFile();
				lastTimePrinted = tick.getCurrentTime();
			} else if (tick.getCurrentTime() >= lastTimePrinted + printingFrequency) {
				writeToFile();
				lastTimePrinted = tick.getCurrentTime();
			}
			return true;
		}
		return false;
	}

	private void writeToFile() {
		try (BufferedWriter writer = Files.newBufferedWriter(logFilePath)) {
			writer.write(printable.toString());
			writer.write("\n\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Processor clone() {
		return null;
	}

}
