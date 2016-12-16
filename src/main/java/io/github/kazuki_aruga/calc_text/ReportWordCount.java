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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author k-aruga
 *
 */
public class ReportWordCount {

	private static final Log log = LogFactory.getLog(ReportWordCount.class);

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			log.info("処理を開始します");

			conn.setAutoCommit(false);

			for (Report report : listReport(conn)) {

				final int wcSec1 = selectWordCount(conn, report, 1);
				final int wcSec2 = selectWordCount(conn, report, 2);
				final int vcSec1 = selectVocabCount(conn, report, 1);
				final int newVcSec1 = selectNewVocabCount(conn, report, 1);
				final int pastRdNewVcSec1 = selectPastRdCount(conn, report);
				final int vcSec2 = selectVocabCount(conn, report, 2);
				final int newVcSec2 = selectNewVocabCount(conn, report, 2);
				final int vcTotal = selectVocabCount(conn, report);

				if (!updateReport(conn, report, wcSec1, wcSec2, wcSec1 + wcSec2, vcSec1, newVcSec1, pastRdNewVcSec1,
						vcSec2, newVcSec2, vcTotal)) {

					log.warn("更新に失敗しました：comp_code=" + report.getCompCode() + ",year=" + report.getYear());
				}

				log.info("更新します：comp_code=" + report.getCompCode() + ",year=" + report.getYear());

				conn.commit();
			}

			log.info("正常に終了しました");
		}
	}

	private static boolean updateReport(Connection conn, Report report, int wcSec1, int wcSec2, int wcTotal, int vcSec1,
			int newVcSec1, int pastRdNewVcSec1, int vcSec2, int newVcSec2, int vcTotal) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"update report set wc_sec1 = ?, wc_sec2 = ?, wc_total = ?, vc_sec1 = ?, new_vc_sec1 = ?, past_rd_new_vc_sec1 = ?, vc_sec2 = ?, new_vc_sec2 = ?, vc_total = ? where comp_code = ? and year = ?")) {

			int i = 0;

			stmt.setInt(++i, wcSec1);
			stmt.setInt(++i, wcSec2);
			stmt.setInt(++i, wcTotal);
			stmt.setInt(++i, vcSec1);
			stmt.setInt(++i, newVcSec1);
			stmt.setInt(++i, pastRdNewVcSec1);
			stmt.setInt(++i, vcSec2);
			stmt.setInt(++i, newVcSec2);
			stmt.setInt(++i, vcTotal);
			stmt.setString(++i, report.getCompCode());
			stmt.setInt(++i, report.getYear());

			return 1 == stmt.executeUpdate();
		}
	}

	private static List<Report> listReport(Connection conn) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("select comp_code, year from report order by comp_code, year")) {

			try (ResultSet rs = stmt.executeQuery()) {

				final List<Report> result = new ArrayList<>();

				while (rs.next()) {

					final Report report = new Report();

					report.setCompCode(rs.getString(1));
					report.setYear(rs.getInt(2));

					result.add(report);
				}

				return result;
			}
		}
	}

	private static int selectWordCount(Connection conn, Report report, int sec) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"select count(*) from report_word r where r.comp_code = ? and year = ? and r.section = ? and exists"
						+ " (select * from vocab where vocab_id = r.vocab_id and available = 1)")) {

			stmt.setString(1, report.getCompCode());
			stmt.setInt(2, report.getYear());
			stmt.setInt(3, sec);

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}
			}
		}

		throw new IllegalStateException("あり得ない状態");
	}

	private static int selectVocabCount(Connection conn, Report report, int sec) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select count(*) from (" //
				+ "select distinct vocab_id from report_word r where comp_code = ? and year = ? and section = ? and exists (" //
				+ "select * from vocab where vocab_id = r.vocab_id and available = 1)" //
				+ ") tmp")) {

			stmt.setString(1, report.getCompCode());
			stmt.setInt(2, report.getYear());
			stmt.setInt(3, sec);

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}
			}
		}

		throw new IllegalStateException("あり得ない状態");
	}

	private static int selectVocabCount(Connection conn, Report report) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select count(*) from (" //
				+ "select distinct vocab_id from report_word r where comp_code = ? and year = ? and exists " //
				+ "(select * from vocab where vocab_id = r.vocab_id and available = 1)" //
				+ ") tmp")) {

			stmt.setString(1, report.getCompCode());
			stmt.setInt(2, report.getYear());

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}
			}
		}

		throw new IllegalStateException("あり得ない状態");
	}

	/**
	 * セクションに初出の語彙の数を返す。
	 * 
	 * @param conn
	 * @param report
	 * @return
	 * @throws SQLException
	 */
	private static int selectNewVocabCount(Connection conn, Report report, int sec) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select count(*) from ( " //
				+ "select distinct vocab_id from report_word r where comp_code = ? and year = ? and section = ? and first_app = 1 and exists " //
				+ "(select * from vocab where vocab_id = r.vocab_id and available = 1)" //
				+ ") tmp")) {

			stmt.setString(1, report.getCompCode());
			stmt.setInt(2, report.getYear());
			stmt.setInt(3, sec);

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}
			}
		}

		throw new IllegalStateException("あり得ない状態");
	}

	private static int selectPastRdCount(Connection conn, Report report) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select count(*) from ( " //
				+ "select distinct vocab_id from report_word r where comp_code = ? and year = ? and section = 1 and first_app = 1 and past_rd = 1 and exists " //
				+ "(select * from vocab where vocab_id = r.vocab_id and available = 1)" //
				+ ") tmp")) {

			stmt.setString(1, report.getCompCode());
			stmt.setInt(2, report.getYear());

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}
			}
		}

		throw new IllegalStateException("あり得ない状態");
	}
}
