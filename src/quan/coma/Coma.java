package quan.coma;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.wdilab.coma.center.Manager;
import de.wdilab.coma.gui.Controller;
import de.wdilab.coma.insert.metadata.XSDParser;
import de.wdilab.coma.matching.Workflow;
import de.wdilab.coma.matching.execution.ExecWorkflow;
import de.wdilab.coma.repository.DataAccess;
import de.wdilab.coma.structure.Graph;
import de.wdilab.coma.structure.MatchResult;

public class Coma {
	
	public static final String DIR="C:\\Users\\NTQuan\\workspace\\Coma\\";
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/coma-project";
	
	static final String USER = "root";
	static final String PASS = "20092137";
	
//	public static void main(String[] args){
//		Coma coma = new Coma();
//		Result[] result = new Result[6];
//		
//		System.out.println("--------------- Result:\n\n\n");
//		result = coma.match1("product.xsd");
//		for(int i=0;i<6;i++){
//			System.out.println("product"+(i+1));
//			System.out.println("ID:"+result[i].getId());
//			System.out.println("Value:"+result[i].getValue()+"\n");
//		}
//	}
	
	public Result[] match1(String schema){
		
		Connection conn=null;
		Statement stmt = null;
		int tmp=0,i=0,j=0,k=0, l=0;
		float matchResult=0.0f;
		String sourceSchemaPath="";
		String targetSchemaPath="";
		Result tmp1, tmp2;
		Result[] result = new Result[6];
		
		
		//init array
		for(i=0;i<6;i++){
			result[i] = new Result();
		}
		
		
		sourceSchemaPath = "C:\\Users\\NTQuan\\workspace\\Coma\\sources\\"+schema;
		
		Coma coma = new Coma();
		
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("Connected database successfully...");
			
			stmt = conn.createStatement();
			String sql = "SELECT * FROM products";
			ResultSet rs = stmt.executeQuery(sql);
			
			while(rs.next()){
				 //bien de gioi han so luong matching
				 if(l==10)
					 break;
				 else
					 l++;
				 
				 
		         int id  = rs.getInt("id");
		         String name = rs.getString("name");
		         targetSchemaPath="C:\\Users\\NTQuan\\workspace\\Coma\\sources\\"+name;
		         
		         //matching
		         matchResult=coma.match(sourceSchemaPath, targetSchemaPath);
//		         System.out.println("Result"+id+":"+coma.match(sourceSchemaPath, targetSchemaPath));
		         j=0;
		         tmp=0;
		         for(j=5;j>=0;j--){
		        	 if(matchResult < result[j].getValue())
		        		 break;
		        	 else
		        		 tmp++;
		         }
		         if(tmp>0){
		        	 if(tmp==6){
		        		 tmp1 = new Result(id, matchResult);
			        	 for(k=0;k<6;k++){
			        		 tmp2 = result[k];
			        		 result[k]=tmp1;
			        		 tmp1=tmp2;
			        	 }
		        	 }else{
		        		 tmp1 = new Result(id, matchResult);
			        	 for(k=j+1;k<6;k++){
			        		 tmp2 = result[k];
			        		 result[k]=tmp1;	 
			        		 tmp1=tmp2;
			        	 }
		        	 }
		        	 
		         }
		         
		      }
		      rs.close();
			
		}catch(SQLException se){
			se.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		// Close connection
		try{
			if(conn!=null){
				conn.close();
				System.out.println("Closed!");
			}
		}catch(SQLException se){
			se.printStackTrace();
		}
		
		
		return result;
	}

	public float match( String sourceSchemaPath, String targetSchemaPath) {
		String tmp="";
		
		/* (1) We connect to the database. */
		System.setProperty( "comaUrl", "jdbc:mysql://localhost/coma-project?autoReconnect=true");
		System.setProperty( "comaUser", "root");
		System.setProperty( "comaPwd", "20092137");
		
		/* (2) We create a new manager and controller. */
		Manager manager = new Manager();
		DataAccess accessor = manager.getAccessor();
		Controller c = new Controller( manager, accessor);
		c.createNewDatabase( false);
		manager = c.getManager();
		accessor = manager.getAccessor();
		
		/* (3) We tell COMA the paths and explain what we want to match. */
		String[][] schemaImport = { { sourceSchemaPath, "library_src"}, 
				{ targetSchemaPath, "library_tar"} };
		String[][] schemaPairs = { {"library_src", "library_tar"} };
		
		/* (4) We execute the workflow, running the ALLCONTEXT strategy. */
		Workflow w = new Workflow( Workflow.ALLCONTEXT);
		ExecWorkflow exec = new ExecWorkflow();
		/* (4.1) We load the two xsdschema using the XSD Parser. */
		boolean dbInsert = true;
		XSDParser xsdParser = new XSDParser( dbInsert);
		for(int i = 0; i < schemaImport.length; i++) {
			String schemaLocation = schemaImport[i][0];
			String schemaName = schemaImport[i][1];
			xsdParser.parseSingleSource(schemaLocation, schemaName);
		}
		/* (4.2) Parsing accomplished - we update the information. */
		c.updateAll( true);  // update information
		manager = c.getManager();
		accessor = manager.getAccessor();
		for( int i = 0; i < schemaPairs.length; i++) {
			String sourceName = schemaPairs[i][0];
			
			
			String targetName = schemaPairs[i][1];
			int sourceId = accessor.getSourceIdsWithName( sourceName).get(0);
			int targetId = accessor.getSourceIdsWithName( targetName).get(0);
			Graph sourceGraph = manager.loadGraph( sourceId);
			Graph targetGraph = manager.loadGraph( targetId);
			w.setSource( sourceGraph); 
			w.setTarget( targetGraph);
			MatchResult[] result = exec.execute(w);  // Match the two schemas
			
			tmp=result[0].toString();
			tmp=tmp.substring(tmp.indexOf("product <-> product:"));
			tmp=tmp.replaceAll("^.*product:", "");
			tmp=tmp.substring(0, tmp.indexOf("+"));
			//System.out.println(result[0]);
			

		}
		return Float.parseFloat(tmp);
	}
}
