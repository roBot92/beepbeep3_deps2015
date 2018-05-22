package hu.bme.mit.beepbeep3;


import java.util.Calendar;
import java.util.List;

import org.junit.Before;

import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.Pushable;
import hu.bme.mit.beepbeep3.processors.InvalidTaxiLogFilterProcessor;
import hu.bme.mit.beepbeep3.processors.task1.ExpiringRoutesComputingProcessor;
import hu.bme.mit.beepbeep3.processors.task1.NewRouteComputingProcessor;
import hu.bme.mit.entities.TaxiLog;
import hu.bme.mit.entities.Tick;
import hu.bme.mit.test.AbstractTask1Test;

public class BeepBeepTask1Test extends AbstractTask1Test{


	private Pushable filterProcPushable;
	private Pushable exProcPushable;


	@Before
	public void setUp() throws Exception {
		super.setUp();	
		InvalidTaxiLogFilterProcessor filterProc = new InvalidTaxiLogFilterProcessor(1, 1, tlog -> {
			return tlog.getPickup_datetime() == null || tlog.getDropoff_datetime() == null
					|| tlog.getPickup_cell() == null || tlog.getDropoff_cell() == null;
		});
		NewRouteComputingProcessor nrProc = new NewRouteComputingProcessor(1, 1, toplist);
		ExpiringRoutesComputingProcessor exProc = new ExpiringRoutesComputingProcessor(2, 0, toplist);
		

		/*
		 * 0 0 0 0 0 0 fProc --->filterProc--->nrProc ---> orProc |1 ^1
		 * |__________________________________|
		 */
		Connector.connect(filterProc, 0, nrProc, 0);
		Connector.connect(nrProc, 0, exProc, 0);

		exProcPushable = exProc.getPushableInput(1);
		filterProcPushable = filterProc.getPushableInput(0);
		



	}

	@Override
	protected void insertTaxiLogs(List<TaxiLog> taxiLogs) {
		filterProcPushable.push(taxiLogs);
		
	}

	@Override
	protected void rollPseudoClock(long time) {
		calendar.add(Calendar.MILLISECOND, (int)time);
		exProcPushable.push(new Tick(calendar.getTimeInMillis()));
		
	}

	@Override
	protected void fireRules() {
		// NOOP
		
	}
	
	

}
