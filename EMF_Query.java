import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

public class EMF_Query{
    String usr ="jiangao";
    String pwd ="";
    String url ="jdbc:postgresql://localhost:5432/sales";

    public class Structure {  //class defined to store database and mf_structure information
        String type;
        String variable_name;
        Structure (){}
        Structure (String t, String v){
        type = t;
        variable_name = v;
        }
    }

    public class Aggr_func {  //aggregate functions   ex:1_sum_quant
    	int index;		  //					  1
    	String aggr_attr;	  //					  quant
    	String aggr_func;	  //					  sum
    	String type;		  //					  int
    }
    public class Condition {   //select condition     ex:1.state='NY'
    	int index;		   //					  1
    	String left;		   //					  state
    	String type;		   //					  String
    	String operator;	   //					  =
    	String right;		   //					  NY
    }

	ArrayList<Structure> db_struct_list = new ArrayList<Structure>();  // list store information get from database
	ArrayList<Structure> mf_struct_list = new ArrayList<Structure>();  // list store mf_structure information
	ArrayList<Aggr_func> aggr_func_list =  new ArrayList<Aggr_func>(); // list store aggregate functions info
	ArrayList<Condition> condition_list = new ArrayList<Condition>();  // list store select condition info

    public static void main(String[] args){
	EMF_Query eMF_Query = new EMF_Query();
	eMF_Query.dbconnect();
	eMF_Query.getDB_struct();
	eMF_Query.readFile();
	eMF_Query.output();
    }

    void dbconnect(){
        try {
        Class.forName("org.postgresql.Driver");     //Loads the required driver
        System.out.println("Success loading Driver!");
        } catch(Exception exception) {
        System.out.println("Fail loading Driver!");
        exception.printStackTrace();
        }
    }
    void getDB_struct(){
    	try {
        Connection con = DriverManager.getConnection(url, usr, pwd);		//connect to the database using the password and username
        System.out.println("Success connecting server!\n");
        Statement st = con.createStatement();		//statement created to execute the query
        /*connect to information schema to get variable type*/
        ResultSet rs_type;                    //resultset object gets the set of values retreived from the database
        String query_type = "SELECT data_type,column_name from information_schema.\"columns\" WHERE \"table_name\"='sales' ";
        rs_type = st.executeQuery(query_type);              //executing the query 
        while(rs_type.next()){
            Structure temp = new Structure();
            if(rs_type.getString(1).equals("character varying"))
                temp.type = "String";
            else if(rs_type.getString(1).equals("integer"))
                temp.type = "int";
            temp.variable_name=rs_type.getString(2);
            db_struct_list.add(temp);
        }
        }catch(SQLException e) {
        System.out.println("Connection URL or username or password errors!");
        e.printStackTrace();
        }
    }

    void readFile(){
        try{
        class Query_struct{
            String [] select_attr;
            int num_grouping_var; 
            String [] grouping_attr;
            String [] aggregate_func;
            String [] grouping_var_range;
       	}
        File file = new File("sampleQuery.txt");
        Scanner input = new Scanner(file);

        //Query process
        Query_struct query_struct = new Query_struct();
        input.nextLine();
        query_struct.select_attr = input.nextLine().split(",");
        
        input.nextLine();
        query_struct.num_grouping_var = Integer.parseInt(input.nextLine());
        
      	input.nextLine();
        query_struct.grouping_attr = input.nextLine().split(",");
            
        input.nextLine();
        query_struct.aggregate_func = input.nextLine().split(",");
          
        input.nextLine();
        String line="";
        while(input.hasNext()){
            line+=input.nextLine();
        }
        query_struct.grouping_var_range = line.split(",");


        //parsing grouping attributes          find type of grouping attribute and add to mf_structure
        for (String str : query_struct.grouping_attr){
            for (Structure db_item : db_struct_list){
       		if (db_item.variable_name.equals(str)){
      	  	    Structure mf_item = new Structure(db_item.type, db_item.variable_name);
                    mf_struct_list.add(mf_item);
                    break;
                }
            }
        }

        //parsing aggregate functions			Split aggregate functions: 1_sum_quant to 1 sum quant
        //						then add to mf_structure list and aggregate functions list
        for (String str : query_struct.aggregate_func){
            String [] s = str.split("_");
            Aggr_func aggr_item = new Aggr_func();
            aggr_item.index = Integer.parseInt(s[0]);
       	    aggr_item.aggr_func = s[1];
     	    aggr_item.aggr_attr = s[2];
            for (Structure db_item : db_struct_list){
        	if (db_item.variable_name.equals(aggr_item.aggr_attr)){
       	   	    aggr_item.type = db_item.type;
        	}
            }
            aggr_func_list.add(aggr_item);

            Structure mf_item = new Structure(aggr_item.type, s[1]+"_"+s[2]+"_"+s[0]);
            mf_struct_list.add(mf_item);
        }

        }catch(Exception e){
        System.out.println("Error!");
        }
    }

    void output(){
    	System.out.println("public class DB_STRUCT {");
        for(Structure temp : db_struct_list){
            System.out.printf("\t"+temp.type + "\t" + temp.variable_name+";"+ "\n");
        }
        System.out.println("}");

        System.out.println("public class MF_STRUCT {");
        for (Structure i : mf_struct_list){
            System.out.printf("\t"+i.type+"\t"+i.variable_name+";"+"\n");
        }
        System.out.println("}");
    }
}
