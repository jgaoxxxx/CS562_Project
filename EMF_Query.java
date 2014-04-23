import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.*;
import java.io.*;

public class EMF_Query{
    String usr ="jiangao";
    String pwd ="";
    String url ="jdbc:postgresql://localhost:5432/sales";

    public class Structure {  //class defined to store database and mf_structure information
        String type;
        String variable_name;
        Structure (){}
        Structure (String type, String variable_name){
            this.type = type;
            this.variable_name = variable_name;
        }
        void output (){
            System.out.printf(type+"\t"+variable_name+"\n");
        }
    }

    public class Aggr_func {   //aggregate functions   ex:1_sum_quant
    	int index;		       //					   1
    	String aggr_attr;	   //					   quant
    	String aggr_func;	   //					   sum
    	String type;		   //					   int
        Aggr_func (){}
        Aggr_func (int index, String aggr_attr, String aggr_func, String type){
            this.index = index;
            this.aggr_attr = aggr_attr;
            this.aggr_func = aggr_func;
            this.type = type;
        }
        void output (){
            System.out.printf(index+"  "+aggr_attr+"  "+aggr_func+"  "+type+"\n");
        }
    }
    public class Condition {   //select condition      ex:1.state='NY'
    	int index;		       //					   1
    	String left;		   //					   state
    	String type;		   //					   String
    	String operator;	   //					   =
    	String right;		   //					   NY
    }

	ArrayList<Structure> db_struct_list = new ArrayList<Structure>();  // list store information get from database
	ArrayList<Structure> mf_struct_list = new ArrayList<Structure>();  // list store mf_structure information
    ArrayList<Structure> group_attr_list = new ArrayList<Structure>(); // list store grouping attributes info
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
        query_struct.select_attr = input.nextLine().split(",");
        query_struct.num_grouping_var = Integer.parseInt(input.nextLine());
        query_struct.grouping_attr = input.nextLine().split(",");
        query_struct.aggregate_func = input.nextLine().split(",");
        String line="";
        while(input.hasNext()){
            line+=input.nextLine();
        }
        query_struct.grouping_var_range = line.split(",");

        //parsing select attributes
        for (String str : query_struct.select_attr){
            //count the number of "_", to make sure the attribute type
            int count=0;
            for(int i=0;i<str.length();i++){
                if(str.charAt(i)== '_'){
                    count++;
                }
            }
            if (count == 0){   // attributes like cust, prod
                for (Structure temp : db_struct_list){
                    if (temp.variable_name.equals(str)){
                        Structure mf_item = new Structure(temp.type, temp.variable_name);
                        mf_struct_list.add(mf_item);
                        break;
                    }
                }
            }
            if (count == 1){   //attributes like 1_month, avg_month
                String [] s = str.split("_");
                for (Structure temp : db_struct_list){
                    if (temp.variable_name.equals(s[1])){
                        Structure mf_item = new Structure(temp.type, s[1]+"_"+s[0]);
                        mf_struct_list.add(mf_item);
                        break;
                    }
                }
            }
            if (count == 2){   //attributes like 1_avg_quant, 2_sum_quant
                String [] s = str.split("_");
                for (Structure temp : db_struct_list){
                    if (temp.variable_name.equals(s[2])){
                        Structure mf_item = new Structure(temp.type, s[2]+"_"+s[0]+"_"+s[1]);
                        mf_struct_list.add(mf_item);
                        break;
                    }
                }
            }
        }


        //parsing grouping attributes
        for (String str : query_struct.grouping_attr){
            for (Structure temp : db_struct_list){
       		if (temp.variable_name.equals(str)){
      	  	    Structure new_group_attr = new Structure(temp.type, temp.variable_name);
                    group_attr_list.add(new_group_attr);
                    break;
                }
            }
        }
        //parsing aggregate functions
        for (String str : query_struct.aggregate_func){
            int count=0;
            for(int i=0;i<str.length();i++){
                if(str.charAt(i)== '_'){
                    count++;
                }
            }
            if (count == 1){   //attributes like avg_month
                String [] s = str.split("_");
                for (Structure temp : db_struct_list){
                    if (temp.variable_name.equals(s[1])){
                        Aggr_func new_aggr = new Aggr_func(0, s[1], s[0], temp.type);
                        aggr_func_list.add(new_aggr);
                        break;
                    }
                }
            }
            if (count == 2){   //attributes like 1_avg_quant, 2_sum_quant
                String [] s = str.split("_");
                for (Structure temp : db_struct_list){
                    if (temp.variable_name.equals(s[2])){
                        Aggr_func new_aggr = new Aggr_func(Integer.parseInt(s[0]), s[2], s[1], temp.type);
                        aggr_func_list.add(new_aggr);
                        break;
                    }
                }
            }
        }

        }catch(Exception e){
        System.out.println("Error!");
        }
    }

    void output(){
    	System.out.println("public class DB_STRUCT {");
        for(Structure temp : db_struct_list)
            temp.output();
        System.out.println("}");

        System.out.println("public class MF_STRUCT {");
        for (Structure temp : mf_struct_list)
            temp.output();
        System.out.println("}");



        //some test of output
        for (Structure temp : group_attr_list)
            temp.output();
        for (Aggr_func temp : aggr_func_list)
            temp.output();
    }
}
