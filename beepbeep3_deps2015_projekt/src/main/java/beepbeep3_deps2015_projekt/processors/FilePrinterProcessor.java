package beepbeep3_deps2015_projekt.processors;


import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.Tick;

public class FilePrinterProcessor extends SingleProcessor {

	private Object printable;
	private Path logFilePath;
	private PrintStream outStream;
	long printingFrequency;
	long lastTimePrinted = -1;
	boolean isStream = false;

	public FilePrinterProcessor(int in_arity, int out_arity, Object printDestination, Object printable, long frequency) {
		super(in_arity, out_arity);
		
		if(printDestination instanceof String) {
			this.logFilePath = Paths.get((String)printDestination);
		} else if(printDestination instanceof OutputStream) {
			outStream = (PrintStream)printDestination;
			isStream = true;
		}
		
		this.printable = printable;
		printingFrequency = frequency;

	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if (input[0] instanceof Tick) {
			Tick tick = (Tick) input[0];
			if (lastTimePrinted == -1) {
				lastTimePrinted = tick.getCurrentTime();
			} 
			if (tick.getCurrentTime() >= lastTimePrinted + printingFrequency) {
				if(isStream) {
					outStream.println(printable);
				} else {
					writeToFile(tick);
				}				
				lastTimePrinted = tick.getCurrentTime();
			}
			return true;
		}
		return false;
	}

	private void writeToFile(Tick tick) {

		try {
			Files.write(logFilePath, (printable.toString() + "\n").getBytes(), StandardOpenOption.APPEND);
			System.out.println("CurrentTime:" + new Date(tick.getCurrentTime()));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public Processor clone() {
		return null;
	}

}
