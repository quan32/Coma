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
	String name, image, price, brand, content="";
	JSONObject json = new JSONObject();
	
	
	//Get prameter
	int id1=Integer.parseInt(request.getParameter("id1"));
	int id2=Integer.parseInt(request.getParameter("id2"));
	int id3=Integer.parseInt(request.getParameter("id3"));
	int id4=Integer.parseInt(request.getParameter("id4"));
	int id5=Integer.parseInt(request.getParameter("id5"));
	int id6=Integer.parseInt(request.getParameter("id6"));
	
	
	//Connect db
	Class.forName("com.mysql.jdbc.Driver");
	con = DriverManager.getConnection(url, "root", "20092137");
	stmt = con.createStatement();
	
	//get product 1
	String sql = "SELECT * FROM products where id="+id1;
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

		json.put("product1", id + "&" + name + "&" + image + "&"+ price + "&" + brand + "&" + content);
	}

	//get product 2
	sql = "SELECT * FROM products where id=" + id2;
	rs = stmt.executeQuery(sql);
	while (rs.next()) {
		id = rs.getInt("id");
		name = rs.getString("fullname");
		image = rs.getString("image");
		image = "images/" + image;
		price = rs.getString("price");
		brand = rs.getString("brand");
		

		json.put("product2", id + "&" + name + "&" + image + "&"
				+ price + "&" + brand);
	}
	//get product 3
	sql = "SELECT * FROM products where id=" + id3;
	rs = stmt.executeQuery(sql);
	while (rs.next()) {
		id = rs.getInt("id");
		name = rs.getString("fullname");
		image = rs.getString("image");
		image = "images/" + image;
		price = rs.getString("price");
		brand = rs.getString("brand");

		json.put("product3", id + "&" + name + "&" + image + "&"
				+ price + "&" + brand);
	}
	//get product 4
	sql = "SELECT * FROM products where id=" + id4;
	rs = stmt.executeQuery(sql);
	while (rs.next()) {
		id = rs.getInt("id");
		name = rs.getString("fullname");
		image = rs.getString("image");
		image = "images/" + image;
		price = rs.getString("price");
		brand = rs.getString("brand");

		json.put("product4", id + "&" + name + "&" + image + "&"
				+ price + "&" + brand);
	}
	//get product 5
	sql = "SELECT * FROM products where id=" + id5;
	rs = stmt.executeQuery(sql);
	while (rs.next()) {
		id = rs.getInt("id");
		name = rs.getString("fullname");
		image = rs.getString("image");
		image = "images/" + image;
		price = rs.getString("price");
		brand = rs.getString("brand");

		json.put("product5", id + "&" + name + "&" + image + "&"
				+ price + "&" + brand);
	}
	//get product 6
	sql = "SELECT * FROM products where id=" + id6;
	rs = stmt.executeQuery(sql);
	while (rs.next()) {
		id = rs.getInt("id");
		name = rs.getString("fullname");
		image = rs.getString("image");
		image = "images/" + image;
		price = rs.getString("price");
		brand = rs.getString("brand");

		json.put("product6", id + "&" + name + "&" + image + "&"
				+ price + "&" + brand);
	}

	//close connection
	con.close();

	//return JSON
	out.print(json);
%>