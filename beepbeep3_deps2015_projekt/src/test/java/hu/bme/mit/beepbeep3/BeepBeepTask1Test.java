package hu.bme.mit.beepbeep3;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.uqac.lif.cep.Pushable;
import hu.bme.mit.beepbeep3.processors.InvalidTaxiLogFilterProcessor;
import hu.bme.mit.beepbeep3.processors.task1.ExpiringRoutesComputingProcessor;
import hu.bme.mit.beepbeep3.processors.task1.NewRouteComputingProcessor;
import hu.bme.mit.entities.Route;
import hu.bme.mit.entities.TaxiLog;
import hu.bme.mit.entities.Tick;
import hu.bme.mit.positioning.Cell;
import hu.bme.mit.toplist.FrequentRoutesToplistSet;

public class BeepBeepTask1Test {

	private FrequentRoutesToplistSet toplist;
	private static List<Cell> cells;
	private static List<TaxiLog> route1tlogs;
	private static List<TaxiLog> route2tlogs;
	private static List<TaxiLog> route3tlogs;
	private Calendar clock;

	private Pushable filterProcPushable;
	private Pushable exProcPushable;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// calendar = Calendar.getInstance();
		cells = Arrays.asList(new Cell(1, 1), new Cell(1, 2), new Cell(2, 1), new Cell(2, 2), new Cell(3, 1),
				new Cell(3, 2));

		route1tlogs = Arrays.asList(setUpTaxilog(cells.get(0), cells.get(1)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(0), cells.get(1)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(0), cells.get(1)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(0), cells.get(1)/* , getZeroTimeCalendar() */));

		route2tlogs = Arrays.asList(setUpTaxilog(cells.get(2), cells.get(3)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(2), cells.get(3)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(2), cells.get(3)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(2), cells.get(3)/* , getZeroTimeCalendar() */));

		route3tlogs = Arrays.asList(setUpTaxilog(cells.get(4), cells.get(5)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(4), cells.get(5)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(4), cells.get(5)/* , getZeroTimeCalendar() */),
				setUpTaxilog(cells.get(4), cells.get(5)/* , getZeroTimeCalendar() */));

	}

	@Before
	public void setUp() throws Exception {
		toplist = new FrequentRoutesToplistSet();
		clock = Calendar.getInstance();
		clock.setTimeInMillis(0);

		for (int i = 0; i < 4; i++) {
			route1tlogs.get(i).setDropoff_datetime(getZeroTimeCalendar());
			route2tlogs.get(i).setDropoff_datetime(getZeroTimeCalendar());
			route3tlogs.get(i).setDropoff_datetime(getZeroTimeCalendar());
		}

		InvalidTaxiLogFilterProcessor filterProc = new InvalidTaxiLogFilterProcessor(1, 1, tlog -> {
			return tlog.getDropoff_datetime() == null || tlog.getPickup_cell() == null
					|| tlog.getDropoff_cell() == null;
		});

		NewRouteComputingProcessor nrProc = new NewRouteComputingProcessor(1, 1, toplist);
		ExpiringRoutesComputingProcessor exProc = new ExpiringRoutesComputingProcessor(2, 1, toplist);

	/*	FilePrinterProcessor fPrintProc = new FilePrinterProcessor(1, 0, System.out, toplist, 60 * 1000);

		Connector.connect(filterProc, 0, nrProc, 0);
		Connector.connect(nrProc, 0, exProc, 0);
		Connector.connect(exProc, 0, fPrintProc, 0);

		exProcPushable = exProc.getPushableInput(1);
		filterProcPushable = filterProc.getPushableInput(0);

		clock = Calendar.getInstance();
		clock.setTimeInMillis(0);*/

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test_insertOneTaxiLog() {
		TaxiLog tlog1 = route1tlogs.get(0);
		Route route = new Route(tlog1.getPickup_cell(), tlog1.getDropoff_cell(), tlog1.getDropoff_datetime(), 1);

		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		filterProcPushable.push(Arrays.asList(tlog1));

		boolean check = route.valueEquals(toplist.get(0)) && toplist.size() == 1;
		assertTrue(check);
	}

	@Test
	public void test_sortByFrequency() {
		TaxiLog tlog1 = route1tlogs.get(0);
		TaxiLog tlog2 = route2tlogs.get(0);
		TaxiLog tlog3 = route1tlogs.get(1);

		Route route1 = new Route(tlog1.getPickup_cell(), tlog1.getDropoff_cell(), tlog1.getDropoff_datetime(), 1);
		Route route2 = new Route(tlog2.getPickup_cell(), tlog2.getDropoff_cell(), tlog2.getDropoff_datetime(), 1);
		Route route3 = new Route(tlog3.getPickup_cell(), tlog3.getDropoff_cell(), tlog3.getDropoff_datetime(), 2);
		exProcPushable.push(new Tick(clock.getTimeInMillis()));

		filterProcPushable.push(Arrays.asList(tlog1, tlog2));

		boolean check = toplist.size() == 2 && route2.valueEquals(toplist.get(0)) && route1.valueEquals(toplist.get(1));
		assertTrue("check1", check);

		filterProcPushable.push(Arrays.asList(tlog3));
		exProcPushable.push(new Tick(clock.getTimeInMillis()));

		check = toplist.size() == 2 && route2.valueEquals(toplist.get(1)) && route3.valueEquals(toplist.get(0));
		assertTrue("check2", check);

	}

	@Test
	public void testAgeing() {

		// First minute +1 route1, +1 route2, +1 route3
		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		filterProcPushable.push(Arrays.asList(route1tlogs.get(0), route2tlogs.get(0), route3tlogs.get(0)));

		Route route1 = new Route(route1tlogs.get(0).getPickup_cell(), route1tlogs.get(0).getDropoff_cell(),
				route1tlogs.get(0).getDropoff_datetime(), 1);
		Route route2 = new Route(route2tlogs.get(0).getPickup_cell(), route2tlogs.get(0).getDropoff_cell(),
				route2tlogs.get(0).getDropoff_datetime(), 1);
		Route route3 = new Route(route3tlogs.get(0).getPickup_cell(), route3tlogs.get(0).getDropoff_cell(),
				route3tlogs.get(0).getDropoff_datetime(), 1);
		assertTrue("check1", toplist.size() == 3 && toplist.get(0).valueEquals(route3)
				&& toplist.get(1).valueEquals(route2) && toplist.get(2).valueEquals(route1));

		// Second minute, +1 route1, +1 route2
		clock.add(Calendar.MINUTE, 1);

		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		route1tlogs.get(1).setDropoff_datetime(new Date(clock.getTimeInMillis()));
		route2tlogs.get(1).setDropoff_datetime(new Date(clock.getTimeInMillis()));
		filterProcPushable.push(Arrays.asList(route1tlogs.get(1), route2tlogs.get(1)));
		route1.setFrequency(2);
		route1.setLastDropoffTime(clock.getTime());
		route2.setFrequency(2);
		route2.setLastDropoffTime(clock.getTime());

		assertTrue("check2", toplist.size() == 3 && toplist.get(0).valueEquals(route2)
				&& toplist.get(1).valueEquals(route1) && toplist.get(2).valueEquals(route3));

		// Third minute +1 route1
		clock.add(Calendar.MINUTE, 1);
		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		route1tlogs.get(2).setDropoff_datetime(new Date(clock.getTimeInMillis()));
		route1.setFrequency(3);
		route1.setLastDropoffTime(clock.getTime());
		filterProcPushable.push(Arrays.asList(route1tlogs.get(2)));
		assertTrue("check3", toplist.size() == 3 && toplist.get(0).valueEquals(route1)
				&& toplist.get(1).valueEquals(route2) && toplist.get(2).valueEquals(route3));

		clock.add(Calendar.MINUTE, 28);
		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		filterProcPushable.push(Collections.emptyList());
		route1.setFrequency(2);
		route2.setFrequency(1);
		assertTrue("check4",
				toplist.size() == 2 && toplist.get(0).valueEquals(route1) && toplist.get(1).valueEquals(route2));

		clock.add(Calendar.MINUTE, 1);
		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		filterProcPushable.push(Collections.emptyList());
		route1.setFrequency(1);

		assertTrue("check5", toplist.size() == 1 && toplist.get(0).valueEquals(route1));

		clock.add(Calendar.MINUTE, 1);
		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		filterProcPushable.push(Collections.emptyList());

		assertTrue("check6", toplist.size() == 0);

	}

	@Test
	public void test_slidingOut() {
		List<TaxiLog> tlogs = Arrays.asList(setUpTaxilog(cells.get(0), cells.get(1)),
				setUpTaxilog(cells.get(0), cells.get(1)), setUpTaxilog(cells.get(1), cells.get(1)),
				setUpTaxilog(cells.get(1), cells.get(1)), setUpTaxilog(cells.get(1), cells.get(2)),
				setUpTaxilog(cells.get(1), cells.get(2)), setUpTaxilog(cells.get(1), cells.get(3)),
				setUpTaxilog(cells.get(1), cells.get(3)), setUpTaxilog(cells.get(1), cells.get(4)),
				setUpTaxilog(cells.get(1), cells.get(4)), setUpTaxilog(cells.get(1), cells.get(5)),
				setUpTaxilog(cells.get(1), cells.get(5)), setUpTaxilog(cells.get(2), cells.get(1)),
				setUpTaxilog(cells.get(2), cells.get(1)), setUpTaxilog(cells.get(2), cells.get(2)),
				setUpTaxilog(cells.get(2), cells.get(2)), setUpTaxilog(cells.get(2), cells.get(3)),
				setUpTaxilog(cells.get(2), cells.get(3)), setUpTaxilog(cells.get(2), cells.get(4)),
				setUpTaxilog(cells.get(2), cells.get(4)), setUpTaxilog(cells.get(2), cells.get(5)));

		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		filterProcPushable.push(tlogs.subList(0, 20));
		

		assertTrue("check1", toplist.size() == 10 && toplist.getSetSize() == 10);
		clock.add(Calendar.MINUTE, 15);

		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		tlogs.get(20).setDropoff_datetime(new Date(clock.getTimeInMillis()));
		filterProcPushable.push(tlogs.subList(20, 21));

		Route route = new Route(cells.get(2), cells.get(5), tlogs.get(20).getDropoff_datetime(), -1);
		route.setFrequency(1);

		assertTrue("check2", toplist.size() == 10 && toplist.getSetSize() == 11 && !toplist.contains(route));

		clock.add(Calendar.MINUTE, 15);
		exProcPushable.push(new Tick(clock.getTimeInMillis()));
		filterProcPushable.push(Collections.emptyList());
		assertTrue("check3", toplist.size() == 1 && toplist.getSetSize() == 1 && toplist.get(0).valueEquals(route));
	}

	private Date getZeroTimeCalendar() {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		return cal.getTime();
	}

	private static TaxiLog setUpTaxilog(Cell startCell, Cell endCell) {
		TaxiLog tlog = new TaxiLog();
		Calendar zeroCalendar = Calendar.getInstance();
		tlog.setPickup_cell(startCell);
		tlog.setDropoff_cell(endCell);

		zeroCalendar.setTimeInMillis(0);
		tlog.setDropoff_datetime(zeroCalendar.getTime());
		return tlog;
	}

}
