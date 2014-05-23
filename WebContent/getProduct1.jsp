<%@page contentType="application/json; charset=UTF-8"%>
<%@ page errorPage="error.jsp"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.json.simple.JSONObject"%>
<%@ page import="org.w3c.dom.*" %>
<%@ page import="javax.xml.parsers.DocumentBuilder" %>
<%@ page import="javax.xml.parsers.DocumentBuilderFactory" %>
<%@ page import="java.io.*"%>


<%
	Statement stmt;
	Connection con;
	String url = "jdbc:mysql://localhost/coma-project";
	int id;
	String name, image, price, brand, fullname, content="";
	JSONObject json = new JSONObject();
	
	
	//Get prameter
	id=Integer.parseInt(request.getParameter("id"));
	
	
	//Connect db
	Class.forName("com.mysql.jdbc.Driver");
	con = DriverManager.getConnection(url, "root", "20092137");
	stmt = con.createStatement();
	
	//get product 1
	String sql = "SELECT * FROM products where id="+id;
	ResultSet rs = stmt.executeQuery(sql);
	while(rs.next()){
        id  = rs.getInt("id");
        name = rs.getString("fullname");
        image = rs.getString("image");
        image = "images/"+image;
        price = rs.getString("price");
        brand = rs.getString("brand");
        
        BufferedReader br = null;
    	
    	try {
    		String sCurrentLine;
    		br = new BufferedReader(new FileReader("C:\\Users\\NTQuan\\workspace\\Coma\\xml\\schema"+id+".xml"));
    	
    		while ((sCurrentLine = br.readLine()) != null) {
    			//out.println(sCurrentLine);
    			content+=sCurrentLine;
    		}
    	
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		try {
    			if (br != null)br.close();
    		} catch (IOException ex) {
    			ex.printStackTrace();
    		}
    	} 

		json.put("product", id + "&" + name + "&" + image + "&"+ price + "&" + brand + "&" + content);
	}

	//close connection
	con.close();

	//return JSON
	out.print(json);
%>