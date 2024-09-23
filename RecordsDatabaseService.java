/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2592153
 *
 */


import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.sql.*;
import javax.sql.rowset.*;

    //Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
    //these clasess are not exported by the module. Instead, one needs to impor
    //javax.sql.rowset.* as above.



public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome   = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){
        
		//TO BE COMPLETED
		serviceSocket = aSocket;
        this.start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop
		
		String tmp = "";
        try {

            //TO BE COMPLETED
            InputStream socketStream = this.serviceSocket.getInputStream();
            InputStreamReader reader = new InputStreamReader(socketStream);
            StringBuffer stringBuffer = new StringBuffer();
            char currentChar;
            while(true){
                currentChar = (char) reader.read();
                if(currentChar == '#'){
                    break;
                }
                stringBuffer.append(currentChar);
            }
            int indexOfSemicolon = stringBuffer.indexOf(";");

            this.requestStr[0] = stringBuffer.substring(0,indexOfSemicolon);
            this.requestStr[1] = stringBuffer.substring(indexOfSemicolon+1);


        }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        } finally {

        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		
		this.outcome = null;
		
		String sql = "SELECT record.title, record.label, record.genre, record.rrp, COUNT(copyid) FROM recordcopy JOIN record ON record.recordid = recordcopy.recordid JOIN recordshop ON recordshop.recordshopid = recordcopy.recordshopid JOIN artist ON artist.artistid = record.artistid WHERE artist.lastname = ? AND recordshop.city = ? GROUP BY record.title, record.label, record.genre, record.rrp;"; //TO BE COMPLETED- Update this line as needed.
		
		
		try {

			//Connect to the database
			//TO BE COMPLETED
            DriverManager.registerDriver(new org.postgresql.Driver());
            Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            PreparedStatement prepStatement = conn.prepareStatement(sql);
            prepStatement.setString(1, this.requestStr[0]);
            prepStatement.setString(2, this.requestStr[1]);



            //Make the query
			//TO BE COMPLETED

            ResultSet rs = prepStatement.executeQuery();

			
			//Process query
			//TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            RowSetFactory aFactory = RowSetProvider.newFactory () ;
            CachedRowSet crs = aFactory.createCachedRowSet() ;
            crs.populate(rs);
            this.outcome = crs;


            StringBuilder sb = new StringBuilder();

            while(this.outcome.next()) {
                String title = this.outcome.getString(1);
                String label = this.outcome.getString(2);
                String genre = this.outcome.getString(3);
                String rrp = this.outcome.getString(4);
                String numCopies = this.outcome.getString(5);
                sb.append(title + " | " + label + " | " + genre + " | " + rrp + " | " + numCopies + "\n");
            }

            if(!sb.isEmpty()){
                sb.setLength(sb.length()-1);
            }
            System.out.println(sb);

            this.outcome.beforeFirst();

			//Clean up
			//TO BE COMPLETED

            rs.close();
            prepStatement.close();
            conn.close();

            if(this.outcome == null){
                flagRequestAttended = false;
            }
            System.out.println(flagRequestAttended);

		} catch (Exception e)
		{
            System.out.println(e);
            flagRequestAttended = false;
        }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
            //Return outcome
            //TO BE COMPLETED
            OutputStream outputStream = this.serviceSocket.getOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(this.outcome);

            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

            //Terminating connection of the service socket
            //TO BE COMPLETED
            this.serviceSocket.close();


        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        } finally {

        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
