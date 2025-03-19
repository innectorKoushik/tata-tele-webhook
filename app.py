from flask import Flask, request, jsonify
import pymysql
from datetime import datetime

app = Flask(__name__)

# ✅ Connect to MySQL Database
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Kingston#1234",
    "database": "lead_db",
}

def insert_into_db(table_name, data):
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        sql = f"""
        INSERT INTO {table_name} (
            callID, dispnumber, caller_id, start_time, answer_stamp, end_time,
            callType, call_duration, destination, status, resource_url, missedFrom, hangup_cause
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            data.get("callID"), data.get("dispnumber"), data.get("caller_id"), 
            data.get("start_time"), data.get("answer_stamp"), data.get("end_time"), 
            data.get("callType"), data.get("call_duration"), data.get("destination"), 
            data.get("status"), data.get("resource_url"), data.get("missedFrom"), 
            data.get("hangup_cause")
        )

        cursor.execute(sql, values)
        conn.commit()
        conn.close()
        print(f"✅ Data inserted into {table_name}")

    except Exception as e:
        print(f"❌ Database error: {str(e)}")

# ✅ Webhook Endpoints
@app.route('/answered_outbound', methods=['POST'])
def answered_outbound():
    data = request.json
    data["callType"] = "answered_outbound"
    print(f"📞 Answered Outbound Call Received: {data}")
    insert_into_db("answered_outbound_calls", data)
    return jsonify({"message": "Answered outbound call received"}), 200

@app.route('/answered_inbound', methods=['POST'])
def answered_inbound():
    data = request.json
    data["callType"] = "answered_inbound"
    print(f"📞 Answered Inbound Call Received: {data}")
    insert_into_db("answered_inbound_calls", data)
    return jsonify({"message": "Answered inbound call received"}), 200

@app.route('/missed_outbound', methods=['POST'])
def missed_outbound():
    data = request.json
    data["callType"] = "missed_outbound"
    print(f"📞 Missed Outbound Call Received: {data}")
    insert_into_db("missed_outbound_calls", data)
    return jsonify({"message": "Missed outbound call received"}), 200

@app.route('/missed_inbound', methods=['POST'])
def missed_inbound():
    data = request.json
    data["callType"] = "missed_inbound"
    print(f"📞 Missed Inbound Call Received: {data}")
    insert_into_db("missed_inbound_calls", data)
    return jsonify({"message": "Missed inbound call received"}), 200

# ✅ Run Flask App
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
