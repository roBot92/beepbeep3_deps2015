package beepbeep3_deps2015_projekt;


import java.util.Queue;

public class FileParserProcessor extends SingleProcessor {

	public FileParserProcessor(int in_arity, int out_arity) {
		super(in_arity, out_arity);
		Context context = this.getContext();

	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> queue) throws ProcessorException {
		queue.add(input);
		return true;
	}

	@Override
	public Processor clone() {
		return new FileParserProcessor(this.getInputArity(), this.getOutputArity());
	}

}