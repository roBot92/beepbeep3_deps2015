package beepbeep3_deps2015_projekt;

import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;

public class ComputingProcessor extends SingleProcessor {

	public ComputingProcessor(int in_arity, int out_arity) {
		super(in_arity, out_arity);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> queue) throws ProcessorException {
		System.out.println("nyehh");
		for(int i = 0 ; i < input.length ; i++) {
			System.out.println(input[i]);
		}
		return false;
	}

	@Override
	public Processor clone() {
		// TODO Auto-generated method stub
		return null;
	}

}
