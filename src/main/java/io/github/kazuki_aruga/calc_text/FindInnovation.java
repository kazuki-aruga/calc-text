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
 * @author kazuki
 */
public class FindInnovation {

	private static final Log log = LogFactory.getLog(FindInnovation.class);

	/**
	 * 2003年以降で新規に出現する単語を探す。<br>
	 * 
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			log.info("処理を開始します");

			conn.setAutoCommit(true);

			for (int vocabId : listAvailableVocab(conn)) {

				final int firstYear = firstAppearYear(conn, vocabId);

				if (2003 <= firstYear) {

					log.info("イノベーションワードを発見した：vocab_id=" + vocabId);

					updateInnovation(conn, vocabId);
				}
			}
		}
	}

	/**
	 * 有効な語彙の語彙IDを検索する。
	 * 
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	private static List<Integer> listAvailableVocab(Connection conn) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("select vocab_id from vocab where available = 1 order by vocab_id")) {

			try (ResultSet rs = stmt.executeQuery()) {

				final List<Integer> result = new ArrayList<>();

				while (rs.next()) {

					result.add(rs.getInt(1));
				}

				return result;
			}
		}
	}

	/**
	 * 語彙の初出年度を取得する。
	 * 
	 * @param conn
	 * @param vocabId
	 * @return
	 * @throws SQLException
	 */
	private static int firstAppearYear(Connection conn, int vocabId) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("select min(year) from report_word where vocab_id = ?")) {

			stmt.setInt(1, vocabId);

			try (ResultSet rs = stmt.executeQuery()) {

				if (rs.next()) {

					return rs.getInt(1);
				}
			}
		}
		throw new IllegalStateException("あり得ない状態");
	}

	/**
	 * イノベーション単語とする。
	 * 
	 * @param conn
	 * @param vocabId
	 * @return
	 * @throws SQLException
	 */
	private static boolean updateInnovation(Connection conn, int vocabId) throws SQLException {

		try (PreparedStatement stmt = conn.prepareStatement("update vocab set innovation = 1 where vocab_id = ?")) {

			stmt.setInt(1, vocabId);

			return 1 == stmt.executeUpdate();
		}
	}

}
