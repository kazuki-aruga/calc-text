/**
 * 
 */
package io.github.kazuki_aruga.calc_text;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * @author k-aruga
 *
 */
public class DocTermCreator {

	/**
	 * @param args
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws SQLException, IOException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			final List<Vocab> vocabList = listVocab(conn);

			final Map<String, int[]> docTerm = new LinkedHashMap<>();

			for (Comp comp : listComp(conn)) {

				for (int year : listYear(conn, comp.getCompCode())) {

					docTerm.put(comp.getCompCode() + "_" + year,
							listWc(conn, vocabList.size(), comp.getCompCode(), year));
				}
			}

			final CSVPrinter print = CSVFormat.EXCEL
					.print(new OutputStreamWriter(new FileOutputStream(args[0]), "MS932"));
			try {

				writeHeader(print, docTerm.keySet());

				for (Vocab vocab : vocabList) {

					if (vocab.isAvailable()) {

						writeRecord(print, vocab, docTerm);
					}
				}

			} finally {

				print.close();
			}
		}
	}

	private static void writeRecord(CSVPrinter print, Vocab vocab, Map<String, int[]> docTerm) throws IOException {

		final List<String> record = new ArrayList<>();

		record.add("" + vocab.getVocabId() + ":" + vocab.getProto() + ":" + vocab.getPos());

		for (int[] term : docTerm.values()) {

			record.add(Integer.toString(term[vocab.getVocabId() - 1]));
		}

		print.printRecord(record);
	}

	private static void writeHeader(CSVPrinter print, Set<String> keySet) throws IOException {

		final List<String> header = new ArrayList<>();

		header.add("");
		header.addAll(keySet);

		print.printRecord(header);
	}

	public static List<Comp> listComp(Connection conn) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("select comp_code, comp_name from comp order by comp_code")) {

			try (ResultSet rs = stmt.executeQuery()) {

				final List<Comp> result = new ArrayList<>();

				while (rs.next()) {

					final Comp comp = new Comp();

					comp.setCompCode(rs.getString("comp_code"));
					comp.setCompName(rs.getString("comp_name"));

					result.add(comp);
				}

				return result;
			}
		}
	}

	public static List<Integer> listYear(Connection conn, String compCode) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("select year from report where comp_code = ? and active = 1 order by year")) {

			stmt.setString(1, compCode);

			try (ResultSet rs = stmt.executeQuery()) {

				final List<Integer> result = new ArrayList<>();

				while (rs.next()) {

					result.add(rs.getInt("year"));
				}

				return result;
			}
		}
	}

	public static List<Vocab> listVocab(Connection conn) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("select vocab_id, proto, pos, available from vocab order by vocab_id")) {

			try (ResultSet rs = stmt.executeQuery()) {

				final List<Vocab> result = new ArrayList<>();

				while (rs.next()) {

					final Vocab vocab = new Vocab();

					vocab.setVocabId(rs.getInt("vocab_id"));
					vocab.setProto(rs.getString("proto"));
					vocab.setPos(rs.getString("pos"));
					vocab.setAvailable(rs.getBoolean("available"));

					result.add(vocab);
				}

				return result;
			}
		}
	}

	public static int[] listWc(Connection conn, int count, String compCode, int year) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"select v.vocab_id, count(r.comp_code) wc from vocab v inner join report_word r on v.vocab_id = r.vocab_id and r.comp_code = ? and r.year = ? where v.available = 1 group by v.vocab_id order by v.vocab_id")) {

			stmt.setString(1, compCode);
			stmt.setInt(2, year);

			try (ResultSet rs = stmt.executeQuery()) {

				final int[] wc = new int[count];

				while (rs.next()) {

					wc[rs.getInt("vocab_id") - 1] = rs.getInt("wc");
				}

				return wc;
			}
		}
	}

}
