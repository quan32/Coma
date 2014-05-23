<%@page import="quan.coma.Result"%>
<%@page import="org.apache.catalina.connector.Request"%>
<%@page import="java.io.*,org.w3c.dom.*,javax.xml.parsers.*,javax.xml.transform.*, javax.xml.transform.dom.*,javax.xml.transform.stream.*,org.xml.sax.InputSource"%>  
<jsp:useBean id="coma" class="quan.coma.Coma" />
<%@ page import="org.json.simple.JSONObject"%>
<%!
	public String path="C:\\Users\\NTQuan\\workspace\\Coma\\sources\\product.xsd";
	private static Document convertStringToDocument(String xmlStr) {
	    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();  
	    DocumentBuilder builder;  
	    try 
	    {  
	        builder = factory.newDocumentBuilder();  
	        Document doc = builder.parse( new InputSource( new StringReader( xmlStr ) ) ); 
	        return doc;
	    } catch (Exception e) {  
	        e.printStackTrace();  
	    } 
	    return null;
	}
	
	public void createXsd(String info) throws Exception{
		try{
		
		    TransformerFactory factory = TransformerFactory.newInstance();
		    Transformer transformer = factory.newTransformer();
		
		    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		
		    String data = info;				
		    Document doc = convertStringToDocument(data);
		
		    
		    StringWriter sw = new StringWriter();
		    StreamResult result = new StreamResult(sw);
		    DOMSource source = new DOMSource(doc);
		    transformer.transform(source, result);
		    String xmlString = sw.toString();
		    
		    File file=new File(path);
		    
		    BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		    bw.write(xmlString);
		    bw.flush();
		    bw.close();
		}
		catch(Exception e)
		{
		System.out.println(e);
		}   
	}
%>
<% 
	String info=request.getParameter("data");
	createXsd(info);
	
	Result[] result = new Result[6];
	JSONObject json = new JSONObject();
	
	result = coma.match();
	for(int i=0;i<6;i++){
		json.put("product"+(i+1), result[i].getId()+"&"+result[i].getValue());
	}
	
	//return JSON
    out.print(json);	
%>