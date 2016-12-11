package io.github.kazuki_aruga.calc_text;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * 
 * @author k-aruga
 */
public class CsvExporter {

	public static void main(String[] args) throws SQLException, IOException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			final CSVPrinter printer = CSVFormat.EXCEL
					.print(new OutputStreamWriter(new FileOutputStream(args[0]), "UTF-8"));
			try {

				try (PreparedStatement stmt = createStatement(conn)) {

					try (ResultSet rs = stmt.executeQuery()) {

						writeHeader(printer, rs.getMetaData());

						printer.printRecords(rs);
					}
				}

			} finally {

				printer.close();
			}
		}
	}

	private static PreparedStatement createStatement(Connection conn) throws SQLException {

		return conn.prepareStatement("select r.comp_code, r.year, r.sales, r.ebitda, r.rd, count(*), r.vc_sec1 "
				+ "from report r inner join report_word w inner join vocab v on r.comp_code = w.comp_code and r.year = w.year and v.vocab_id = w.vocab_id "
				+ "where r.year > 2000 and r.year < 2015 and r.active = 1 and v.available = 1 and w.section = 1 " + "group by r.comp_code, r.year");
	}

	/**
	 * 
	 * @param writer
	 * @param meta
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void writeHeader(CSVPrinter writer, ResultSetMetaData meta) throws SQLException, IOException {

		final String[] names = new String[meta.getColumnCount()];

		for (int i = 0; i < names.length; i++) {

			names[i] = meta.getColumnName(i + 1);
		}

		writer.printRecord((Object[]) names);
	}

}
