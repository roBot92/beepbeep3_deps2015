package beepbeep3_deps2015_projekt;

import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.Tick;
import onlab.utility.DataFileParser;

public class FileParserProcessor extends SingleProcessor {

	private DataFileParser parser;

	public FileParserProcessor(int in_arity, int out_arity, DataFileParser parser) {
		super(in_arity, out_arity);
		this.parser = parser;

	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> queue) throws ProcessorException {
		if (parser == null) {
			return false;
		}
		if (input[0] instanceof Tick && parser.hasNextLine()) {
			Object[] output = parser.parseNextLinesFromCSVGroupedByDropoffDate().toArray();
			queue.add(output);

			System.out.println(output);
			return true;
		} else if (!parser.hasNextLine()) {
			try {
				parser.close();
			} catch (Exception e) {
				System.out.println(e);
			}
			return false;
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

}