import mysql.connector



# Step 2: Insert a string into the table
def insert_string(data):
    # Step 1: Connect to MySQL
    connection = mysql.connector.connect(
        host="192.168.1.2",
        port=3306, 
        user="root",
        password="Shreya@22",
        database="mysql_test"
    )

    cursor = connection.cursor()
    try:
        if not connection.is_connected():
            print("Reconnecting to MySQL...")
            connection.connect()  # Reconnect if not connected
            cursor = connection.cursor()  # Reinitialize cursor
        query = """
        INSERT INTO test_table (source_type, source_name, temperature, pressure, timestamp, tempF, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            data['source_type'],
            data['source_name'],
            data['temperature'],
            data['pressure'],
            data['timestamp'],
            data['tempF'],
            data['date']
        ))
        
       # cursor.execute(query, (data,))
        connection.commit()
        print("String inserted successfully." , data)
        return data
        
    except mysql.connector.Error as e:
        print("Error while inserting data:", e)



    # Step 3: Close the connection
    cursor.close()
    connection.close()
