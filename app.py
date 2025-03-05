import time
import hmac
import hashlib
import base64
import json
import requests
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

API_KEY = "e2b91e6f-cc7f-4e62-95a1-2ffc7dde6c63"
API_SECRET = "75579DA822EFFF6455F89943722719A5"
PASSPHRASE = "Crs@00148600"
BASE_URL = "https://www.okx.com"

app = Flask(__name__)

def generate_signature(timestamp, method, request_path, body=""):
    body = body if body else ''
    message = f"{timestamp}{method}{request_path}{body}"
    mac = hmac.new(API_SECRET.encode(), message.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

def make_request(method, endpoint, params=None, data=None):
    url = BASE_URL + endpoint
    timestamp = "{:.3f}".format(time.time())
    body = json.dumps(data) if data else ""
    headers = {
        "OK-ACCESS-KEY": API_KEY,
        "OK-ACCESS-SIGN": generate_signature(timestamp, method, endpoint, body),
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": PASSPHRASE,
        "Content-Type": "application/json"
    }
    response = requests.request(method, url, headers=headers, params=params, data=body)
    return response.json()

@app.route("/balance", methods=["GET"])
def get_balance():
    response = make_request("GET", "/api/v5/account/balance")
    if "data" in response and response["data"]:
        for asset in response["data"][0]["details"]:
            if asset["ccy"] == "BRL":
                return jsonify({"BRL_available": asset["availBal"]})
    return jsonify({"error": "BRL balance not found"}), 404

@app.route("/buy", methods=["POST"])
def buy_nc():
    balance_response = make_request("GET", "/api/v5/account/balance")
    if "data" not in balance_response or not balance_response["data"]:
        return jsonify({"error": "Could not fetch account balance"}), 400
    
    brl_available = 0
    usdt_available = 0
    for asset in balance_response["data"][0]["details"]:
        if asset["ccy"] == "BRL":
            brl_available = float(asset["availBal"])
        if asset["ccy"] == "USDT":
            usdt_available = float(asset["availBal"])

    if brl_available < 20 and usdt_available < 10:
        return jsonify({
            "error": "Insufficient balance. Both BRL and USDT balances are too low.",
            "BRL_available": brl_available,
            "USDT_available": usdt_available
        }), 400
    
    usdt_after_conversion = usdt_available
    if brl_available >= 20:
        convert_data = {
            "instId": "USDT-BRL",
            "tdMode": "cash",
            "side": "buy",
            "ordType": "market",
            "sz": str(brl_available)
        }
        convert_response = make_request("POST", "/api/v5/trade/order", data=convert_data)
        if "data" not in convert_response or not convert_response["data"]:
            return jsonify({"error": "BRL to USDT conversion failed", "details": convert_response}), 400
        
        if "data" in convert_response and isinstance(convert_response["data"], list):
            conversion_data = convert_response["data"][0]
            if "filledSz" in conversion_data:
                usdt_after_conversion = float(conversion_data["filledSz"])
            elif "sz" in conversion_data:
                usdt_after_conversion = float(conversion_data["sz"])
            else:
                return jsonify({
                    "error": "Unexpected response format for BRL to USDT conversion",
                    "response": conversion_data
                }), 400

    if usdt_after_conversion < 10:
        return jsonify({
            "error": "Insufficient USDT balance for purchase.",
            "available_usdt": usdt_after_conversion
        }), 400

    instrument_id = "NC-USDT"
    order_data = {
        "instId": instrument_id,
        "tdMode": "cash",
        "side": "buy",
        "ordType": "market",
        "sz": str(usdt_after_conversion)
    }

    buy_response = make_request("POST", "/api/v5/trade/order", data=order_data)

    if "data" in buy_response and buy_response["data"]:
        return jsonify({
            "order_id": buy_response["data"][0]["ordId"],
            "instrument_id": buy_response["data"][0]["instId"],
            "status": buy_response["data"][0]["state"],
            "filled_size": buy_response["data"][0]["filledSz"]
        })
    
    return jsonify({"error": "Failed to place buy order", "response": buy_response}), 400

def job_buy_nc():
    with app.test_client() as client:
        response = client.post("/buy")
        print(f"Job executed at {datetime.now()}: {response.json}")

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(job_buy_nc, 'interval', hours=3)  # Run every 3 hours
    scheduler.start()

    app.run(debug=True)
