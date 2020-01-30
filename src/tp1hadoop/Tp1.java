package tp1hadoop;

import java.io.FileReader;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class Tp1 {

	public static void main(String[] args) {
		Reader in;
		try {
			in = new FileReader("arbresremarquablesparis2011.csv");
			Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
			for (CSVRecord record : records) {
			    String lastName = record.get(0);
			    String firstName = record.get(1);
			    System.out.println(lastName + " " + firstName);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
