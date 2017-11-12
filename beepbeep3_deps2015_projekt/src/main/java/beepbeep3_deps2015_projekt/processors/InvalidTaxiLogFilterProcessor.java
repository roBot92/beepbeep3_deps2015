package beepbeep3_deps2015_projekt.processors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.function.Predicate;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.TaxiLog;

public class InvalidTaxiLogFilterProcessor extends SingleProcessor {

	private Predicate<TaxiLog> filter;
	public InvalidTaxiLogFilterProcessor(int in_arity, int out_arity, Predicate<TaxiLog> filter) {
		super(in_arity, out_arity);
		this.filter = filter;
	}
	

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if(input[0] instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<TaxiLog> tlogs = new ArrayList<TaxiLog>((List<TaxiLog>) input[0]);
			Iterator<TaxiLog> iterator = tlogs.iterator();
			
			while(iterator.hasNext()) {
				TaxiLog tlog = iterator.next();
				if(filter.test(tlog)) {
					iterator.remove();
				}
			}
			output.add(new Object[] {tlogs});
			return true;
		}
		
		return false;
	}

	@Override
	public Processor clone() {
		// TODO Auto-generated method stub
		return null;
	}

}
