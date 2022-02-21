package edu.jsu.mcis.cs310;

import java.sql.*;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Database {
    
    private final Connection connection;
    
    private final int TERMID_SP22 = 1;
    
    /* CONSTRUCTOR */

    public Database(String username, String password, String address) {
        
        this.connection = openConnection(username, password, address);
        
    }
    
    /* PUBLIC METHODS */

    public String getSectionsAsJSON(int termid, String subjectid, String num) {
        
       String result = null, query;
        
        // INSERT YOUR CODE HERE
        ResultSet resultset = null;
        boolean hasresults;
        PreparedStatement pstSelect = null;

        try {
            if (isConnected()) {

                /* Prepare Select Query */
                query = "SELECT * FROM jsu_sp22_v1.section s WHERE subjectid = ? AND num = ?";
                pstSelect = connection.prepareStatement(query);
                pstSelect.setString(1, subjectid);
                pstSelect.setString(2, num);

                // Executes Select Query //
                hasresults = pstSelect.execute();

                // Checks for Results //
                if (hasresults) {

                    // Gets Results set //
                    resultset = pstSelect.getResultSet();

                    // Encodes to JSON //
                    result = getResultSetAsJSON(resultset);

                }
                // If no data is returned, prints error //
                else {
                    System.err.println("Error: No data returned!");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
        
    }
    
    public int register(int studentid, int termid, int crn) {
        
        int result = 0;
        
        // INSERT YOUR CODE HERE
        int updateCount;
        String query;
        PreparedStatement pstUpdate = null;
        try {

            if (isConnected()) {

                // Prepares Insert Query //
                query = "INSERT INTO jsu_sp22_v1.registration VALUES (?,?,?)";
                pstUpdate = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
                pstUpdate.setInt(1, studentid);
                pstUpdate.setInt(2, termid);
                pstUpdate.setInt(3, crn);

                // Executes Insert Query //
                updateCount = pstUpdate.executeUpdate();
               // Updates Results //
                if (updateCount > 0) {
                    result = updateCount;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
        
    }

    public int drop(int studentid, int termid, int crn) {
        
        int result = 0;
        
        // INSERT YOUR CODE HERE
        
        int updateCount;
        String query;
        PreparedStatement pstUpdate = null;
        try {

            if (isConnected()) {

                // Prepares Insert Query //
                query = "DELETE FROM jsu_sp22_v1.registration  WHERE studentid = ? AND termid = ? AND crn = ?";
                pstUpdate = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
                pstUpdate.setInt(1, studentid);
                pstUpdate.setInt(2, termid);
                pstUpdate.setInt(3, crn);

                // Executes Insert Query //
                updateCount = pstUpdate.executeUpdate();
                // Updates Results //
                if (updateCount > 0) {
                    result = updateCount;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
        
    }
    
    public int withdraw(int studentid, int termid) {
        
        int result = 0;
        
        // INSERT YOUR CODE HERE
        int updateCount;
        String query;
        PreparedStatement pstUpdate = null;
        try {

            if (isConnected()) {

               // Prepares Insert Query //
                query = "DELETE FROM jsu_sp22_v1.registration  WHERE studentid = ? AND termid = ? ";
                pstUpdate = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
                pstUpdate.setInt(1, studentid);
                pstUpdate.setInt(2, termid);

                // Executes Insert Query //
                updateCount = pstUpdate.executeUpdate();
                // Updates Results //
                if (updateCount > 0) {
                    result = updateCount;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
        
    }
    
    public String getScheduleAsJSON(int studentid, int termid) {
        
        String result = null, query;
        
        // INSERT YOUR CODE HERE
        ResultSet resultset = null;
        boolean hasresults;
        PreparedStatement pstSelect = null;

        try {
            if (isConnected()) {

                // Prepares the Select Query //
                query = "SELECT * FROM jsu_sp22_v1.registration r JOIN jsu_sp22_v1.section ON r.crn = jsu_sp22_v1.section.crn AND r.studentid = ? AND r.termid = ?";
                pstSelect = connection.prepareStatement(query);
                pstSelect.setInt(1, studentid);
                pstSelect.setInt(2, termid);

                // Executes the Select Query //
                hasresults = pstSelect.execute();

                // Checks for Results //
                if (hasresults) {

                    // Gets the Results set //
                    resultset = pstSelect.getResultSet();

                    // Encodes to JSON //
                    result = getResultSetAsJSON(resultset);

                }
                // Prints error if theres no data returned //
                else {
                    System.err.println("Error: No data returned!");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
        
    }
    
    public int getStudentId(String username) {
        
        int id = 0;
        
        try {
        
            String query = "SELECT * FROM student WHERE username = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, username);
            
            boolean hasresults = pstmt.execute();
            
            if ( hasresults ) {
                
                ResultSet resultset = pstmt.getResultSet();
                
                if (resultset.next())
                    
                    id = resultset.getInt("id");
                
            }
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return id;
        
    }
    
    public boolean isConnected() {

        boolean result = false;
        
        try {
            
            if ( !(connection == null) )
                
                result = !(connection.isClosed());
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    /* PRIVATE METHODS */

    private Connection openConnection(String u, String p, String a) {
        
        Connection c = null;
        
        if (a.equals("") || u.equals("") || p.equals(""))
            
            System.err.println("*** ERROR: MUST SPECIFY ADDRESS/USERNAME/PASSWORD BEFORE OPENING DATABASE CONNECTION ***");
        
        else {
        
            try {

                String url = "jdbc:mysql://" + a + "/jsu_sp22_v1?autoReconnect=true&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=America/Chicago";
                // System.err.println("Connecting to " + url + " ...");

                c = DriverManager.getConnection(url, u, p);

            }
            catch (Exception e) { e.printStackTrace(); }
        
        }
        
        return c;
        
    }
    
    private String getResultSetAsJSON(ResultSet resultset) {
        
        String result;
        
        /* Create JSON Containers */
        
        JSONArray json = new JSONArray();
        JSONArray keys = new JSONArray();
        
        try {
            
            /* Get Metadata */
        
            ResultSetMetaData metadata = resultset.getMetaData();
            int columnCount = metadata.getColumnCount();
            
            /* Get Keys */
            
            for (int i = 1; i <= columnCount; ++i) {

                keys.add(metadata.getColumnLabel(i));

            }
            
            /* Get ResultSet Data */
            
            while(resultset.next()) {
                
                /* Create JSON Container for New Row */
                
                JSONObject row = new JSONObject();
                
                /* Get Row Data */

                for (int i = 1; i <= columnCount; ++i) {
                    
                    /* Get Value; Pair with Key */

                    Object value = resultset.getObject(i);
                    row.put(keys.get(i - 1), String.valueOf(value));

                }
                
                /* Add Row Data to Collection */
                
                json.add(row);

            }
        
        }
        catch (Exception e) { e.printStackTrace(); }
        
        /* Encode JSON Data and Return */
        
        result = JSONValue.toJSONString(json);
        return result;
        
    }
    
}