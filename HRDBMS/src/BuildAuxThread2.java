import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;


public class BuildAuxThread2 extends Thread
{
	private int num;
	private int nodes;
	
	public BuildAuxThread2(int num, int nodes)
	{
		this.num = num;
		this.nodes = nodes;
	}
	
	public void run()
	{
		Connection[] cons = new Connection[nodes];
	
		try
		{
			Class.forName("com.ibm.db2.jcc.DB2Driver");
		
			int i = 0;
			while (i < nodes)
			{
				cons[i] = DriverManager.getConnection(
					"jdbc:db2://db2" + (i+1) + ":50000/TPCH", 
					"db2inst1",
					"db2inst1");
				cons[i].setAutoCommit(false);
				i++;
			}
		
			Statement stmt = cons[num].createStatement();
			stmt.executeUpdate("TRUNCATE TABLE TPCD.CUSTOMER_AUX IMMEDIATE");
			cons[num].commit();
			PreparedStatement stmt2 = cons[num].prepareStatement("INSERT INTO TPCD.CUSTOMER_AUX (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) VALUES(?,?,?,?,?,?,?,?)");
			ResultSet rs = stmt.executeQuery("SELECT DISTINCT O_CUSTKEY FROM TPCD.ORDERS WHERE O_CUSTKEY NOT IN (SELECT C_CUSTKEY FROM TPCD.CUSTOMER)");
			
			i = 0;
			String sql = "SELECT * FROM TPCD.CUSTOMER WHERE C_CUSTKEY IN (-1";
			while (rs.next())
			{
				sql += ("," + rs.getInt(1));
				i++;
				
				if (i % 5000 == 0)
				{
					System.out.println("CUSTOMER_AUX(" + num + ") - " + i + " rows");
					sql += ")";
					int j = 0;
					SQLThread[] threads = new SQLThread[nodes];
					while (j < nodes)
					{
						if (j != num)
						{
							threads[j] = new SQLThread(cons[j], sql);
							threads[j].start();
						}
						else
						{
							threads[j] = new SQLThread(cons[j], "SELECT * FROM TPCD.CUSTOMER WHERE 1 = 0");
							threads[j].start();
						}
						j++;
					}
					
					j = 0;
					while (j < nodes)
					{
						stmt2.clearBatch();
						
						threads[j].join();
						while (threads[j].rs.next())
						{
							stmt2.setInt(1, threads[j].rs.getInt(1));
							stmt2.setString(2, threads[j].rs.getString(2));
							stmt2.setString(3, threads[j].rs.getString(3));
							stmt2.setInt(4, threads[j].rs.getInt(4));
							stmt2.setString(5, threads[j].rs.getString(5));
							stmt2.setFloat(6,  threads[j].rs.getFloat(6));
							stmt2.setString(7, threads[j].rs.getString(7));
							stmt2.setString(8, threads[j].rs.getString(8));
							stmt2.addBatch();
						}
						
						threads[j].rs.close();
						threads[j].stmt.close();
						
						if (j != num)
						{
							try
							{
								Statement stmt3 = cons[num].createStatement();
								stmt3.execute("ALTER TABLE TPCD.CUSTOMER_AUX ACTIVATE NOT LOGGED INITIALLY");
								stmt2.executeBatch();
								cons[num].commit();
							}
							catch(BatchUpdateException e)
							{
								System.out.println(e);
								System.out.println(e.getNextException());
								cons[num].rollback();
								System.exit(0);
							}
						}
						j++;
					}
					
					sql = "SELECT * FROM TPCD.CUSTOMER WHERE C_CUSTKEY IN (-1";
				}
			}
			
			sql += ")";
			int j = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (j < nodes)
			{
				if (j != num)
				{
					threads[j] = new SQLThread(cons[j], sql);
					threads[j].start();
				}
				else
				{
					threads[j] = new SQLThread(cons[j], "SELECT * FROM TPCD.CUSTOMER WHERE 1 = 0");
					threads[j].start();
				}
				j++;
			}
			
			j = 0;
			while (j < nodes)
			{
				stmt2.clearBatch();
				threads[j].join();
				while (threads[j].rs.next())
				{
					stmt2.setInt(1, threads[j].rs.getInt(1));
					stmt2.setString(2, threads[j].rs.getString(2));
					stmt2.setString(3, threads[j].rs.getString(3));
					stmt2.setInt(4, threads[j].rs.getInt(4));
					stmt2.setString(5, threads[j].rs.getString(5));
					stmt2.setFloat(6,  threads[j].rs.getFloat(6));
					stmt2.setString(7, threads[j].rs.getString(7));
					stmt2.setString(8, threads[j].rs.getString(8));
					stmt2.addBatch();
				}
				
				if (j != num)
				{
					Statement stmt3 = cons[num].createStatement();
					stmt3.execute("ALTER TABLE TPCD.CUSTOMER_AUX ACTIVATE NOT LOGGED INITIALLY");
					stmt2.executeBatch();
					cons[num].commit();
				}
				j++;
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
}
