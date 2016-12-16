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
public class UpdatePastRd {

	private static final Log log = LogFactory.getLog(UpdatePastRd.class);

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			conn.setAutoCommit(false);

			for (Report report : listReport(conn)) {

				log.info("処理を開始します:comp_code=" + report.getCompCode() + ",year=" + report.getYear());

				final int count = updatePastRd(conn, report);

				log.info("更新しました:update_count=" + count);

				conn.commit();
			}

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

	private static int updatePastRd(Connection conn, Report report) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"update report_word set past_rd = 1 where comp_code = ? and year = ? and section = 1 and first_app = 1 and vocab_id in ( select vocab_id from ( "
						+ " select distinct vocab_id from report_word where comp_code = ? and year <= ? and section = 2) tmp)")) {

			int i = 0;

			stmt.setString(++i, report.getCompCode());
			stmt.setInt(++i, report.getYear());
			stmt.setString(++i, report.getCompCode());
			stmt.setInt(++i, report.getYear());

			return stmt.executeUpdate();
		}
	}

}
