package beepbeep3_deps2015_projekt;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.TaxiLog;

public class InvalidTaxiLogFilter extends SingleProcessor {

	
	public InvalidTaxiLogFilter(int in_arity, int out_arity) {
		super(in_arity, out_arity);
	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if(input[0] instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<TaxiLog> tlogs = (List<TaxiLog>) input[0];
			Iterator<TaxiLog> iterator = tlogs.iterator();
			
			while(iterator.hasNext()) {
				TaxiLog tlog = iterator.next();
				if(tlog.getPickup_datetime() == null || tlog.getDropoff_datetime() == null || tlog.getPickup_cell() == null || tlog.getDropoff_cell() == null) {
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
