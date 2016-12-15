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

		// 満足化基準のデータ
		// return conn.prepareStatement("select comp_code, year, sales, ebitda,
		// rd, wc_sec1, vc_sec1 "
		// + "from report where year > 2000 and year < 2015 and active = 1");

		// 課題のデータ
		return conn.prepareStatement(
				"select r.comp_code, r.year, r.sales, r.rd, r.ebitda, r.vc_sec1, r.new_vc_sec1 from report r inner join ( "
						+ "select comp_code, max(year) max_year, min(year) min_year, count(*) cnt from report where active = 1 and year < 2015 group by comp_code having count(*) > 10) t "
						+ "on r.comp_code = t.comp_code " + "where r.year >= t.min_year + 3");
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
