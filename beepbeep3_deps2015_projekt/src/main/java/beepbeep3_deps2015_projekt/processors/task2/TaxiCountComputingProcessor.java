package beepbeep3_deps2015_projekt.processors.task2;

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
import onlab.utility.ProfitableAreaToplistSet;

public class TaxiCountComputingProcessor extends SingleProcessor {

	private ProfitableAreaToplistSet toplist;
	private Map<String, TaxiMovedEntry> actualTaxiLocations = new HashMap<String, TaxiMovedEntry>();
	private Queue<TaxiMovedEntry> taxiMovements = new LinkedList<TaxiMovedEntry>();

	private static long lengthOfTimeWindow = 30 * 60 * 1000;

	private class TaxiMovedEntry {
		private String license;
		private Cell cell;
		private long time;

		public TaxiMovedEntry(String license, Cell cell, long time) {
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

			handleExpiringTaxiLogs((Tick) input[1]);
			handleIncomingTaxiLogs((List<TaxiLog>) input[0]);

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
			TaxiMovedEntry previousEntryOfTaxi = actualTaxiLocations.get(taxiLicense);
			if (previousEntryOfTaxi != null) {
				toplist.decreaseAreaTaxiCount(previousEntryOfTaxi.cell, dropoffTime);
			}
			TaxiMovedEntry newEntryOfTaxi = new TaxiMovedEntry(taxiLicense, cell, dropoffTime.getTime());
			actualTaxiLocations.put(taxiLicense, newEntryOfTaxi);
			taxiMovements.add(newEntryOfTaxi);

		}
	}

	private void handleExpiringTaxiLogs(Tick tick) {
		long currentTime = tick.getCurrentTime();
		while (taxiMovements.peek() != null && taxiMovements.peek().time < currentTime - lengthOfTimeWindow) {
			TaxiMovedEntry entry = taxiMovements.poll();
			//Ha a taxi aktuális hely entryje megegyezik a kifutó entryvel, akkor kell csak csökkenteni (mert akkor nem tartozik hozzá újabb) és ebben az esetben törölni is lehet
			//Az aktuális lokációját
			if (entry == actualTaxiLocations.get(entry.license)) {
				toplist.decreaseAreaTaxiCount(entry.cell, null);
				actualTaxiLocations.remove(entry.license);
			}

		}
	}

}
