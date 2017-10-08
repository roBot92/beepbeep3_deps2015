package beepbeep3_deps2015_projekt;

import java.util.List;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.Route;
import onlab.event.TaxiLog;
import onlab.utility.FrequentRoutesToplistSet;

public class NewRouteComputingProcessor extends SingleProcessor {

	private FrequentRoutesToplistSet<Route> toplistSet;
	public NewRouteComputingProcessor(int in_arity, int out_arity, FrequentRoutesToplistSet<Route> toplistSet) {
		super(in_arity, out_arity);
		this.toplistSet = toplistSet;
	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> queue) throws ProcessorException {
		
		
		
		if(input[0] instanceof List<?>) {
			List<?> newTlogs = (List<?>)input[0];
			for(Object o : newTlogs) {
				TaxiLog tlog = (TaxiLog) o;
				toplistSet.increaseRouteFrequency(tlog.getPickup_cell(), tlog.getPickup_cell(), tlog.getDropoff_datetime());
			}
			queue.add(input);
		}
		
		return true;
	}

	@Override
	public Processor clone() {
		// TODO Auto-generated method stub
		return null;
	}

}
