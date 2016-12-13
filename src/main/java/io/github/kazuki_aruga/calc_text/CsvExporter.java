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

		return conn.prepareStatement("select comp_code, year, sales, ebitda, rd, wc_sec1, vc_sec1 "
				+ "from report where year > 2000 and year < 2015 and active = 1");
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
