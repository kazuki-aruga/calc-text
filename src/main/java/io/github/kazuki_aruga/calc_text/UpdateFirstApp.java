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
public class UpdateFirstApp {

	private static final Log log = LogFactory.getLog(UpdateFirstApp.class);

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

				final int sec1 = updateFirstApp(conn, report, 1);
				final int sec2 = updateFirstApp(conn, report, 2);

				log.info("更新しました:sec1=" + sec1 + ",sec2=" + sec2);

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

	private static int updateFirstApp(Connection conn, Report report, int sec) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"update report_word set first_app = 1 where comp_code = ? and year = ? and section = ? and vocab_id not in ( select vocab_id from ( "
						+ " select distinct vocab_id from report_word where comp_code = ? and year < ? and section = ?) tmp)")) {

			stmt.setString(1, report.getCompCode());
			stmt.setInt(2, report.getYear());
			stmt.setInt(3, sec);
			stmt.setString(4, report.getCompCode());
			stmt.setInt(5, report.getYear());
			stmt.setInt(6, sec);

			return stmt.executeUpdate();
		}
	}

}
