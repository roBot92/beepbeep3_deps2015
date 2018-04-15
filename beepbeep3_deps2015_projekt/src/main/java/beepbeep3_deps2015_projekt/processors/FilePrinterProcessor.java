package beepbeep3_deps2015_projekt.processors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.Tick;
import onlab.main.DebsMain;
import onlab.utility.ToplistSetInterface;

public class FilePrinterProcessor extends SingleProcessor {

	private ToplistSetInterface toplist;
	private int runningMode;
	private String previousToplistWithoutDelay;
	private BufferedWriter bufferedFileWriter;
	FileParserProcessor fParserProc = null;
	private Runtime runtime;
	private long benchmarkFrequency;
	private long previousTime = -1;
	private long startingTime = -1;

	public FilePrinterProcessor(int in_arity, int out_arity, ToplistSetInterface toplist, int runningMode,
			BufferedWriter bufferedFileWriter, long benchmarkFrequency, FileParserProcessor fParserProc) {
		super(in_arity, out_arity);
		this.toplist = toplist;
		this.runningMode = runningMode;
		this.bufferedFileWriter = bufferedFileWriter;
		this.runtime = Runtime.getRuntime();
		this.benchmarkFrequency = benchmarkFrequency;
		this.fParserProc = fParserProc;
		
	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if (input[0] instanceof Tick) {
			Tick tick = (Tick) input[0];
			if(startingTime == -1){
				startingTime = tick.getCurrentTime();
				DebsMain.restartCurrentTime();
			}
			try {
				previousToplistWithoutDelay = DebsMain.handlePrintActions(toplist, runningMode,
						previousToplistWithoutDelay, bufferedFileWriter, tick.getCurrentTime(), fParserProc.getCounter(), startingTime,
						benchmarkFrequency, runtime);
			} catch (IOException e) {
				e.printStackTrace();
				throw new ProcessorException(e);
			}
			return true;
		}
		return false;
	}

	@Override
	public Processor clone() {
		return null;
	}

	@Override
	public Processor duplicate() {
		return null;
	}

}
