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
				final int vcSec2 = selectVocabCount(conn, report, 2);
				final int vcTotal = selectVocabCount(conn, report);

				if (!updateReport(conn, report, wcSec1, wcSec2, wcSec1 + wcSec2, vcSec1, vcSec2, vcTotal)) {

					log.warn("更新に失敗しました：comp_code=" + report.getCompCode() + ",year=" + report.getYear());
				}

				log.info("更新します：comp_code=" + report.getCompCode() + ",year=" + report.getYear());

				conn.commit();
			}

			log.info("正常に終了しました");
		}
	}

	private static boolean updateReport(Connection conn, Report report, int wcSec1, int wcSec2, int wcTotal, int vcSec1,
			int vcSec2, int vcTotal) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"update report set wc_sec1 = ?, wc_sec2 = ?, wc_total = ?, vc_sec1 = ?, vc_sec2 = ?, vc_total = ? where comp_code = ? and year = ?")) {

			stmt.setInt(1, wcSec1);
			stmt.setInt(2, wcSec2);
			stmt.setInt(3, wcTotal);
			stmt.setInt(4, vcSec1);
			stmt.setInt(5, vcSec2);
			stmt.setInt(6, vcTotal);
			stmt.setString(7, report.getCompCode());
			stmt.setInt(8, report.getYear());

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
				"select count(*) from report_word r inner join vocab v on r.vocab_id = v.vocab_id where v.available = 1 and r.comp_code = ? and year = ? and r.section = ?;")) {

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

		try (PreparedStatement stmt = conn.prepareStatement(
				"select count(*) from (select v.vocab_id from report_word r inner join vocab v on r.vocab_id = v.vocab_id where v.available = 1 and r.comp_code = ? and year = ? and r.section = ? group by v.vocab_id) tmp")) {

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

		try (PreparedStatement stmt = conn.prepareStatement(
				"select count(*) from (select v.vocab_id from report_word r inner join vocab v on r.vocab_id = v.vocab_id where v.available = 1 and r.comp_code = ? and year = ? group by v.vocab_id) tmp")) {

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
