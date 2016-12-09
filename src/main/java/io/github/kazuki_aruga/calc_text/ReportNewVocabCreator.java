/**
 * 
 */
package io.github.kazuki_aruga.calc_text;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author k-aruga
 */
public class ReportNewVocabCreator {

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			conn.setAutoCommit(true);

			for (String compCode : listCompCode(conn)) {

				final List<Integer> yearList = listYear(conn, compCode);
				for (int i = 1, last = yearList.size(); i < last; i++) {

					final int year = yearList.get(i);

					for (ReportNewVocab rnv : listReportNewVocab(conn, compCode, year)) {

						insert(conn, rnv);
					}
				}
			}
		}
	}

	public static List<String> listCompCode(Connection conn) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select comp_code from comp order by comp_code")) {

			try (ResultSet rs = stmt.executeQuery()) {

				final List<String> result = new ArrayList<>();

				while (rs.next()) {

					result.add(rs.getString("comp_code"));
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

	public static List<ReportNewVocab> listReportNewVocab(Connection conn, String compCode, int year)
			throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement( //
				"select r.comp_code, r.year, r.vocab_id, count(*) wc " + //
						"from report_word r inner join vocab v " + //
						"on r.vocab_id = v.vocab_id " + //
						"where r.comp_code = ? and r.year = ? and v.available = 1 " + //
						" and r.vocab_id in ( " + //
						"  select r2.vocab_id " + //
						"  from report_word r1 right outer join report_word r2 " + //
						"  on r1.comp_code = r2.comp_code and r1.year - 1 = r2.year and r1.vocab_id = r2.vocab_id " + //
						"  where r2.comp_code = ? and r2.year = ? " + //
						"  group by r2.vocab_id having count(r1.vocab_id) = 0 " + //
						" ) " + //
						"group by r.comp_code, r.year, r.vocab_id")) {

			stmt.setString(1, compCode);
			stmt.setInt(2, year);
			stmt.setString(3, compCode);
			stmt.setInt(4, year);

			try (ResultSet rs = stmt.executeQuery()) {

				final List<ReportNewVocab> result = new ArrayList<>();

				while (rs.next()) {

					final ReportNewVocab rnv = new ReportNewVocab();

					rnv.setCompCode(rs.getString("comp_code"));
					rnv.setYear(rs.getInt("year"));
					rnv.setVocabId(rs.getInt("vocab_id"));
					rnv.setWc(rs.getInt("wc"));

					result.add(rnv);
				}

				return result;
			}
		}
	}

	public static boolean insert(Connection conn, ReportNewVocab rnv) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("insert into report_new_vocab (comp_code, year, vocab_id, wc) values (?, ?, ?, ?)")) {

			stmt.setString(1, rnv.getCompCode());
			stmt.setInt(2, rnv.getYear());
			stmt.setInt(3, rnv.getVocabId());
			stmt.setInt(4, rnv.getWc());

			return 1 == stmt.executeUpdate();
		}
	}

}
