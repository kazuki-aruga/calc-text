package io.github.kazuki_aruga.calc_text;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Hello world!
 *
 */
public class App {

	private static final Log log = LogFactory.getLog(App.class);

	public static void main(String[] args) throws SQLException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			log.info("処理を開始します");

			conn.setAutoCommit(false);

			for (int i = minVocabId(conn), last = maxVocabId(conn); i <= last; i++) {

				log.info("検索を開始します：vocab_id=" + i);

				final int sections = countSections(conn, i);
				final int reports = countReports(conn, i);

				log.warn("登録を開始します：vocab_id=" + i + ",sections=" + sections + ",reports=" + reports);

				if (!updateVocab(conn, i, sections, reports)) {

					log.warn("更新できませんでした");
				}
			}

			conn.commit();
		}
	}

	private static int countSections(Connection conn, int vocabId) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"select count(*) from (select comp_code, year, section from report_word where vocab_id = ? group by comp_code, year, section) tmp")) {

			stmt.setInt(1, vocabId);

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}

				return 0;
			}
		}
	}

	private static int countReports(Connection conn, int vocabId) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement(
				"select count(*) from (select comp_code, year, section from report_word where vocab_id = ? group by comp_code, year) tmp")) {

			stmt.setInt(1, vocabId);

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}

				return 0;
			}
		}
	}

	private static int minVocabId(Connection conn) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select min(vocab_id) from vocab")) {

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}

				return 0;
			}
		}
	}

	private static int maxVocabId(Connection conn) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select max(vocab_id) from vocab")) {

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}

				return 0;
			}
		}
	}

	private static boolean updateVocab(Connection conn, int vocabId, int sections, int reports) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("update vocab set sections = ?, reports = ? where vocab_id = ?")) {

			stmt.setInt(1, sections);
			stmt.setInt(2, reports);
			stmt.setInt(3, vocabId);

			return 1 == stmt.executeUpdate();
		}
	}

}
