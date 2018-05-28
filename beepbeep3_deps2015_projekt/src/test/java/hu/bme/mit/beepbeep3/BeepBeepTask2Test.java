package hu.bme.mit.beepbeep3;

import java.util.Calendar;
import java.util.List;

import org.junit.Before;

import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.Pushable;
import hu.bme.mit.beepbeep3.processors.InvalidTaxiLogFilterProcessor;
import hu.bme.mit.beepbeep3.processors.task2.MedianComputingProcessor;
import hu.bme.mit.beepbeep3.processors.task2.TaxiCountComputingProcessor;
import hu.bme.mit.entities.TaxiLog;
import hu.bme.mit.entities.Tick;
import hu.bme.mit.test.AbstractTask2Test;
import hu.bme.mit.toplist.ProfitableAreaToplistSet;

public class BeepBeepTask2Test extends AbstractTask2Test{


	private Pushable filterProcPushable;
	private Pushable medProcPushable;



	@Before
	public void setUp() throws Exception {
		super.setUp();
		toplist = new ProfitableAreaToplistSet();
		InvalidTaxiLogFilterProcessor filterProc = new InvalidTaxiLogFilterProcessor(1, 1, tlog -> {
			return tlog.getPickup_datetime() == null || tlog.getDropoff_datetime() == null
					|| tlog.getPickup_cell() == null || tlog.getDropoff_cell() == null || tlog.getFare_amount() == null
					|| tlog.getTip_amount() == null;
		});

		MedianComputingProcessor medProc = new MedianComputingProcessor(2, 2, toplist);
		TaxiCountComputingProcessor countProc = new TaxiCountComputingProcessor(2, 0, toplist);
		

		Connector.connect(filterProc, 0, medProc, 0);
		Connector.connect(medProc, 0, countProc, 0);
		Connector.connect(medProc, 1, countProc, 1);

		medProcPushable = medProc.getPushableInput(1);
		filterProcPushable = filterProc.getPushableInput(0);

	}



	@Override
	protected void insertTaxiLogs(List<TaxiLog> taxiLogs) {
		filterProcPushable.push(taxiLogs);
		
	}



	@Override
	protected void rollPseudoClock(long time) {
		calendar.add(Calendar.MILLISECOND, (int)time);
		medProcPushable.push(new Tick(calendar.getTimeInMillis()));		
	}



	@Override
	protected void fireRules() {
		// NOOP
		
	}

	

	
}
