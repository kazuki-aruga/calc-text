/**
 * 
 */
package io.github.kazuki_aruga.calc_text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author k-aruga
 */
public class ExceptPos {

	private static final Log log = LogFactory.getLog(ExceptPos.class);

	public static void main(String[] args) throws SQLException, UnsupportedEncodingException, IOException {

		try (Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/text-analyzer?autoReconnect=true&useSSL=false", "text-analyzer",
				"text-analyzer")) {

			log.info("処理を開始します");

			conn.setAutoCommit(false);

			try (BufferedReader reader = new BufferedReader(
					new InputStreamReader(ClassLoader.getSystemResourceAsStream("除外品詞.txt"), "UTF-8"))) {

				for (String line = null; (line = reader.readLine()) != null;) {

					final String[] items = line.split("\t");
					if (items.length < 4) {

						continue;
					}

					final int count = updateVocab(conn, items);

					log.info("更新します：pos=" + items[0] + ",pos1=" + items[1] + ",pos2=" + items[2] + ",pos3=" + items[3]
							+ ",count=" + count);

					conn.commit();
				}
			}
		}
	}

	private static int updateVocab(Connection conn, String[] items) throws SQLException {

		try (PreparedStatement stmt = conn
				.prepareStatement("update vocab set except = 1 where pos = ? and pos1 = ? and pos2 = ? and pos3 = ?")) {

			stmt.setString(1, items[0]);
			stmt.setString(2, items[1]);
			stmt.setString(3, items[2]);
			stmt.setString(4, items[3]);

			return stmt.executeUpdate();
		}
	}

}
