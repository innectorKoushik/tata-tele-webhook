from flask import Flask, request, jsonify
import pyodbc
import os
from datetime import datetime

app = Flask(__name__)

# ✅ Azure SQL Database Configuration (Use Environment Variables)
db_config = {
    "server": os.getenv("DB_SERVER", "kei-sql-server.database.windows.net"),
    "database": os.getenv("DB_NAME", "lead_db"),
    "username": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "YourStrongPassword"),
    "driver": "{ODBC Driver 17 for SQL Server}",
}

# ✅ Connection String for Azure SQL
conn_str = f"DRIVER={db_config['driver']};SERVER={db_config['server']};DATABASE={db_config['database']};UID={db_config['username']};PWD={db_config['password']}"

# ✅ Function to Insert Data into Azure SQL
def insert_into_db(table_name, data):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        def parse_datetime(value):
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value else None

        sql = f"""
        INSERT INTO {table_name} (
            callID, dispnumber, caller_id, start_time, answer_stamp, end_time,
            callType, call_duration, destination, status, resource_url, missedFrom, hangup_cause
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        values = (
            data.get("callID"), data.get("dispnumber"), data.get("caller_id"),
            parse_datetime(data.get("start_time")), parse_datetime(data.get("answer_stamp")),
            parse_datetime(data.get("end_time")), data.get("callType"),
            data.get("call_duration"), data.get("destination"), data.get("status"),
            data.get("resource_url"), data.get("missedFrom"), data.get("hangup_cause")
        )

        cursor.execute(sql, values)
        conn.commit()
        print(f"✅ Data inserted into {table_name}")

    except pyodbc.Error as e:
        print(f"❌ Database Error: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ✅ Webhook Endpoints
@app.route('/')
def home():
    return "Tata Tele Webhook Receiver is Running", 200

@app.route('/answered_outbound', methods=['POST'])
def answered_outbound():
    data = request.json
    if not data: return jsonify({"error": "No data received"}), 400

    data["callType"] = "answered_outbound"
    print(f"📞 Answered Outbound Call Received: {data}")
    insert_into_db("answered_outbound_calls", data)
    return jsonify({"message": "Answered outbound call received"}), 200

@app.route('/answered_inbound', methods=['POST'])
def answered_inbound():
    data = request.json
    if not data: return jsonify({"error": "No data received"}), 400

    data["callType"] = "answered_inbound"
    print(f"📞 Answered Inbound Call Received: {data}")
    insert_into_db("answered_inbound_calls", data)
    return jsonify({"message": "Answered inbound call received"}), 200

@app.route('/missed_outbound', methods=['POST'])
def missed_outbound():
    data = request.json
    if not data: return jsonify({"error": "No data received"}), 400

    data["callType"] = "missed_outbound"
    print(f"📞 Missed Outbound Call Received: {data}")
    insert_into_db("missed_outbound_calls", data)
    return jsonify({"message": "Missed outbound call received"}), 200

@app.route('/missed_inbound', methods=['POST'])
def missed_inbound():
    data = request.json
    if not data: return jsonify({"error": "No data received"}), 400

    data["callType"] = "missed_inbound"
    print(f"📞 Missed Inbound Call Received: {data}")
    insert_into_db("missed_inbound_calls", data)
    return jsonify({"message": "Missed inbound call received"}), 200

# ✅ Run Flask App
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)), debug=True)
