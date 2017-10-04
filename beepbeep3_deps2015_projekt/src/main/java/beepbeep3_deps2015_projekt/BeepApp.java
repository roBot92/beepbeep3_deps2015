package beepbeep3_deps2015_projekt;

import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.Connector.ConnectorException;
import ca.uqac.lif.cep.Pushable;
import ca.uqac.lif.cep.interpreter.Interpreter.ParseException;
import onlab.event.TaxiLog;

public class BeepApp {

	public static String FILENAME = "C:\\Users\\Boti\\workspace\\beepbeep3_deps2015_projekt\\src\\main\\resources\\testcsv.csv";

	public static void main(String[] args) throws ParseException, ConnectorException {

		FileParserProcessor fproc = new FileParserProcessor(1,1);
		ComputingProcessor cproc = new ComputingProcessor(1,1);
		Connector.connect(fproc,  cproc);
		
		Pushable pushable = fproc.getPushableInput();
		
		pushable.push(new TaxiLog());

	}

}
