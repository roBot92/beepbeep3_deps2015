package hu.bme.mit.beepbeep3.processors.task1;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import hu.bme.mit.entities.TaxiLog;
import hu.bme.mit.entities.Tick;
import hu.bme.mit.positioning.Cell;
import hu.bme.mit.toplist.FrequentRoutesToplistSet;
public class ExpiringRoutesComputingProcessor extends SingleProcessor {

	private Queue<CellPairEntry> cellPairs = new LinkedList<CellPairEntry>();

	private FrequentRoutesToplistSet toplist = new FrequentRoutesToplistSet();
	// milliseconds, 30 min
	private static long lengthOfTimeWindow = 30 * 60 * 1000;

	private class CellPairEntry {
		private Cell pickupCell;
		private Cell dropoffCell;
		private long dropoffTime;

		public CellPairEntry(Cell pickupCell, Cell dropoffCell, long dropoffTime) {
			this.pickupCell = pickupCell;
			this.dropoffCell = dropoffCell;
			this.dropoffTime = dropoffTime;
		}

	}

	public ExpiringRoutesComputingProcessor(int in_arity, int out_arity, FrequentRoutesToplistSet toplist) {
		super(in_arity, out_arity);
		this.toplist = toplist;

	}

	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if (!(input[0] instanceof List<?>) || !(input[1] instanceof Tick)) {
			return false;
		}
		@SuppressWarnings("unchecked")
		// Adding new entries to the queue
		List<TaxiLog> taxiLogs = (List<TaxiLog>) input[0];
		for (TaxiLog tlog : taxiLogs) {
			cellPairs.add(new CellPairEntry(tlog.getPickup_cell(), tlog.getDropoff_cell(),
					tlog.getDropoff_datetime().getTime()));
		}

		long currentTime = ((Tick) input[1]).getCurrentTime();

		// Decreasing frequencies of routes based on old entries
		while (cellPairs.peek() != null && cellPairs.peek().dropoffTime < currentTime - lengthOfTimeWindow) {
			CellPairEntry entry = cellPairs.poll();
			if (toplist != null) {
				toplist.decreaseRouteFrequency(entry.pickupCell, entry.dropoffCell);
			}

		}

		output.add(new Object[] {input[1]});
		return true;
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
