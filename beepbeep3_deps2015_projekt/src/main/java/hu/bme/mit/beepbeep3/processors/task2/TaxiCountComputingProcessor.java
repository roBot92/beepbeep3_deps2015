package hu.bme.mit.beepbeep3.processors.task2;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.SingleProcessor;
import hu.bme.mit.entities.TaxiLog;
import hu.bme.mit.entities.Tick;
import hu.bme.mit.positioning.Cell;
import hu.bme.mit.toplist.ProfitableAreaToplistSet;

public class TaxiCountComputingProcessor extends SingleProcessor {

	private ProfitableAreaToplistSet toplist;
	private Map<String, TaxiLocationEntry> actualTaxiLocations = new HashMap<String, TaxiLocationEntry>();
	private Queue<TaxiLocationEntry> taxiLocationQueue = new LinkedList<TaxiLocationEntry>();

	private static long lengthOfTimeWindow = 30 * 60 * 1000;

	private class TaxiLocationEntry {
		private String license;
		private Cell cell;
		private long time;

		public TaxiLocationEntry(String license, Cell cell, long time) {
			this.license = license;
			this.cell = cell;
			this.time = time;

		}

	}

	public TaxiCountComputingProcessor(int in_arity, int out_arity, ProfitableAreaToplistSet toplist) {
		super(in_arity, out_arity);

		this.toplist = toplist;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected boolean compute(Object[] input, Queue<Object[]> output) throws ProcessorException {
		if (input[0] instanceof List<?> && input[1] instanceof Tick) {

			List<TaxiLog> tlogs = (List<TaxiLog>) input[0];
			Tick tick = (Tick) input[1];
			handleExpiringTaxiLogs(tick);
			handleIncomingTaxiLogs(tlogs);
		
			
			
			output.add(new Object[] {tick});

			return true;
		}
		return false;
	}

	@Override
	public Processor clone() {
		return null;
	}

	private void handleIncomingTaxiLogs(List<TaxiLog> newTlogs) {
		for (TaxiLog tlog : newTlogs) {
			String taxiLicense = tlog.getHack_license();
			Cell cell = tlog.getDropoff_cell();
			Date dropoffTime = tlog.getDropoff_datetime();
			TaxiLocationEntry previousEntryOfTaxi = actualTaxiLocations.get(taxiLicense);
			if (previousEntryOfTaxi != null) {
				toplist.decreaseAreaTaxiCount(previousEntryOfTaxi.cell, null);
			}
			toplist.increaseAreaTaxiCount(tlog.getDropoff_cell(), tlog.getDropoff_datetime());
			toplist.refreshInsertedForDelay(tlog.getInserted(), tlog.getDropoff_cell());
			TaxiLocationEntry newEntryOfTaxi = new TaxiLocationEntry(taxiLicense, cell, dropoffTime.getTime());
			actualTaxiLocations.put(taxiLicense, newEntryOfTaxi);
			taxiLocationQueue.add(newEntryOfTaxi);

		}
	}

	private void handleExpiringTaxiLogs(Tick tick) {
		long currentTime = tick.getCurrentTime();
		while (taxiLocationQueue.peek() != null && taxiLocationQueue.peek().time < currentTime - lengthOfTimeWindow) {
			TaxiLocationEntry entry = taxiLocationQueue.poll();
			//Ha a taxi aktuális hely entryje megegyezik a kifutó entryvel, akkor kell csak csökkenteni (mert akkor nem tartozik hozzá újabb) és ebben az esetben törölni is lehet
			//Az aktuális lokációját
			if (entry == actualTaxiLocations.get(entry.license)) {
				toplist.decreaseAreaTaxiCount(entry.cell, null);
				actualTaxiLocations.remove(entry.license);
			}

		}
	}

	@Override
	public Processor duplicate() {
		return null;
	}

	

}
