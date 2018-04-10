package beepbeep3_deps2015_projekt.processors.task1;

import java.util.List;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.TaxiLog;
import onlab.utility.FrequentRoutesToplistSet;

public class NewRouteComputingProcessor extends SingleProcessor {

	private FrequentRoutesToplistSet toplistSet;

	public NewRouteComputingProcessor(int in_arity, int out_arity, FrequentRoutesToplistSet toplistSet) {
		super(in_arity, out_arity);
		this.toplistSet = toplistSet;
	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> queue) throws ProcessorException {

		if (input[0] instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<TaxiLog> newTlogs = (List<TaxiLog>) input[0];
			for(TaxiLog tlog : newTlogs) {
				toplistSet.increaseRouteFrequency(tlog.getPickup_cell(), tlog.getDropoff_cell(),
						tlog.getDropoff_datetime(), tlog.getInserted());
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

	@Override
	public Processor duplicate() {
		// TODO Auto-generated method stub
		return null;
	}

}
