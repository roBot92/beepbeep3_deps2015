package beepbeep3_deps2015_projekt.processors.task2;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import onlab.event.TaxiLog;
import onlab.event.Tick;
import onlab.positioning.Cell;
import onlab.utility.CustomTreeMultiset;
import onlab.utility.ProfitableAreaToplistSet;

public class MedianComputingProcessor extends SingleProcessor {

	private ProfitableAreaToplistSet toplist;
	private Map<Cell, CustomTreeMultiset> medianMap = new HashMap<Cell, CustomTreeMultiset>();
	private Queue<MedianElementEntry> cellPairs = new LinkedList<MedianElementEntry>();
	private static long lengthOfTimeWindow = 15 * 60 * 1000;

	private class MedianElementEntry {
		private Cell cell;
		private BigDecimal profit;
		private long dropoffTime;

		public MedianElementEntry(Cell cell, BigDecimal profit, long dropoffTime) {
			super();
			this.cell = cell;
			this.profit = profit;
			this.dropoffTime = dropoffTime;
		}

	}

	// Two imputs, 1 -> TaxiLogs, 2 -> Tick
	public MedianComputingProcessor(int in_arity, int out_arity, ProfitableAreaToplistSet toplist) {
		super(in_arity, out_arity);
		this.toplist = toplist;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if (input[0] instanceof List<?> && input[1] instanceof Tick) {
			Tick tick = (Tick)input[1];
			List<TaxiLog> tlogs = (List<TaxiLog>) input[0];
			
			addNewValuesToMedians((List<TaxiLog>) input[0]);
			removeOldMedianValueElements((Tick) input[1]);			
			output.add(new Object[] {tlogs, tick});
			
			
			return true;
		}

		return false;

	}

	private void addNewValuesToMedians(List<TaxiLog> newTlogs) {
		for (TaxiLog tlog : newTlogs) {
			Cell pickupCell = tlog.getPickup_cell();
			CustomTreeMultiset medianListOfTlog = medianMap.get(tlog.getPickup_cell());
			if (medianListOfTlog == null) {
				medianListOfTlog = new CustomTreeMultiset();
				medianMap.put(pickupCell, medianListOfTlog);
			}
			BigDecimal profit = tlog.getFare_amount().add(tlog.getTip_amount());
			medianListOfTlog.add(profit);
			toplist.refreshAreaMedian(pickupCell, tlog.getDropoff_datetime(), medianListOfTlog.getMedian());
			cellPairs.add(new MedianElementEntry(pickupCell, profit, tlog.getDropoff_datetime().getTime()));
		}
	}

	private void removeOldMedianValueElements(Tick tick) {
		long currentTime = tick.getCurrentTime();

		
		while (cellPairs.peek() != null && cellPairs.peek().dropoffTime <= currentTime - lengthOfTimeWindow) {
			MedianElementEntry entry = cellPairs.poll();
			CustomTreeMultiset medianListOfTlog = medianMap.get(entry.cell);
			if (medianListOfTlog != null) {
				medianListOfTlog.remove(entry.profit);
				toplist.refreshAreaMedian(entry.cell, new Date(tick.getCurrentTime()), medianListOfTlog.getMedian());
			}

		}
	}

	@Override
	public Processor clone() {
		return null;
	}

}
