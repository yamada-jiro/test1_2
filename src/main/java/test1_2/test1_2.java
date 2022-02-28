package test1_2;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class test1_2 {

	public static void main(String[] args) throws Throwable {

		// connect to postgres
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "test", "test");

		// delete h2 file
		new File("test.mv.db").delete();

		// conntect to h2
		Class.forName("org.h2.Driver");
		Connection con2 = DriverManager.getConnection("jdbc:h2:./test", "sa", "");

		// initial sql for h2
		final String[] sqls = {
				"create table test(dir varchar(100), name varchar(100), data BINARY LARGE OBJECT, width int, type int, primary key(dir,name,width));",
				"create table test2(id varchar(100), password bytea, public bytea, private bytea, primary key(id));",
				"create table test3(id varchar(100), dir varchar(100), description varchar(100), common bytea, primary key(id,dir));",
				"create table test4(commenthash varchar(128), primary key(commenthash));",
				"insert into test4 values('bf6f26c550e0d710fba1b440b5ee7e84de2cf9b3364a4bbff5cb49c5891b9f44198e224c69f78de00c7ad0b658e8bb964c91389671de36945681f52c5b9ed8ad');"
		};
		for (int i = 0; i < sqls.length; i++) {
			PreparedStatement stmt = con2.prepareStatement(sqls[i]);
			stmt.execute();
			stmt.close();
		}

		// migration test2
		PreparedStatement ps = con.prepareStatement("select id,password,public,private from test2");
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			PreparedStatement ps2 = con2.prepareStatement("insert into test2 values(?,?,?,?)");
			ps2.setObject(1, rs.getObject(1));
			ps2.setObject(2, rs.getObject(2));
			ps2.setObject(3, rs.getObject(3));
			ps2.setObject(4, rs.getObject(4));
			ps2.execute();
			ps2.close();
		}
		System.out.println("+");
		rs.close();
		ps.close();

		// migration test3
		ps = con.prepareStatement("select id,dir,description,common from test3");
		rs = ps.executeQuery();
		while (rs.next()) {
			PreparedStatement ps2 = con2.prepareStatement("insert into test3 values(?,?,?,?)");
			ps2.setObject(1, rs.getObject(1));
			ps2.setObject(2, rs.getObject(2));
			ps2.setObject(3, rs.getObject(3));
			ps2.setObject(4, rs.getObject(4));
			ps2.execute();
			ps2.close();
		}
		rs.close();
		ps.close();
		System.out.println("-");

		Statement stmt = con.createStatement();
		rs = stmt.executeQuery("select distinct dir from test");

		ArrayList<String> dirs = new ArrayList<String>();
		while (rs.next()) {
			dirs.add(rs.getString("dir"));
			System.out.print("*");
		}
		rs.close();
		stmt.close();
		System.out.println();

		// migration test
		for (String dir : dirs) {
			ps = con.prepareStatement("select dir , name , data, width, type from test where dir = ?");
			ps.setString(1, dir);
			rs = ps.executeQuery();
			while (rs.next()) {
				PreparedStatement ps2 = con2.prepareStatement("insert into test values(?,?,?,?,?)");
				ps2.setObject(1, rs.getObject(1));
				ps2.setObject(2, rs.getObject(2));
				ps2.setBinaryStream(3, rs.getBinaryStream(3));
				ps2.setObject(4, rs.getObject(4));
				ps2.setObject(5, rs.getObject(5));
				ps2.execute();
				ps2.close();
			}
			System.out.print(".");
			rs.close();
			ps.close();
		}
		System.out.println();

		con.close();
		con2.close();

	}

}
