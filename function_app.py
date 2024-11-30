import azure.functions as func
import logging
import urllib.request
import json
import os
import ssl
import ast
import asyncio
import os
import logging
import json
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient


from sklearn.preprocessing import StandardScaler, OneHotEncoder
from haversine import haversine # for location
from category_encoders import TargetEncoder

import pickle

# Replace 'your_file.pkl' with the path to your pickle file
with open('pickle-files/global_model.pkl', 'rb') as file:
    global_model = joblib.load(file)

    # Load the encoders and scaler from pickle files
with open('pickle-files/onehot_encoder.pkl', 'rb') as file:
    onehot_encoder = joblib.load(file)

with open('pickle-files/standard_scaler.pkl', 'rb') as file:
    standard_scaler = joblib.load(file)

with open('pickle-files/target_encoder.pkl', 'rb') as file:
    target_encoder = joblib.load(file)





EVENT_HUB_CONNECTION_STR = os.environ['EVENT_HUB_CONNECTION_STR']
EVENT_HUB_NAME = os.environ['EVENT_HUB_NAME']

async def run(result):
    producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
        )
    
    async with producer:
                logging.info(result)
                json_message = json.dumps(result)
                event_data = EventData(json_message)

                try:
                    await producer.send_batch([event_data])
                
                except Exception as e:
                    print("Send failed: {e}")


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

AML_API_KEY_GLOBAL_MODEL = os.environ.get('AML_API_KEY_GLOBAL_MODEL')

def allowSelfSignedHttps(allowed):
    # bypass the server certificate verification on client side
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True) # this line is needed if you use self-signed certificate in your scoring service.



def call_score_endpoint(url: str,data:dict)->list:
    # call score endpoint
    body = str.encode(json.dumps(data))    
    # Replace this with the primary/secondary key, AMLToken, or Microsoft Entra ID token for the endpoint
    api_key = AML_API_KEY_GLOBAL_MODEL # CHANGE HERE!
    if not api_key:
        raise Exception("A key should be provided to invoke the endpoint")


    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

    req = urllib.request.Request(url, body, headers)

    try:
        response = urllib.request.urlopen(req)

        result = response.read()
        decoded_result = result.decode('utf-8')  # Assuming the bytes are encoded in UTF-8
        actual_result = ast.literal_eval(decoded_result)[0]
        return actual_result #int 

    except urllib.error.HTTPError as error:
        logging.error("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        logging.error(error.info())
        logging.error(error.read().decode("utf8", 'ignore'))


def score(data):
    """
    This function is called for every invocation of the endpoint to perform the actual scoring/prediction.
    In the example we extract the data from the json input and call the scikit-learn model's predict()
    method and return the result back
    """
    try:
        logging.info("model 1: request received")
        # Convert JSON string to dictionary
        data = json.loads(raw_data)

        # Convert dictionary to pandas DataFrame
        df = pd.DataFrame([data])

        ##################
        # Pre-processing # - copying from train.py and EDA
        ##################
        df = df.rename(columns={
        'cc_num': 'bank_account_num',
        'merch_lat': 'transaction_latitude',
        'merch_long': 'transaction_longitude'
        })

        # formatting bank account num as string
        df['bank_account_num'] = df['bank_account_num'].astype(str)
        # rename columns and drop zip and state
        df = df.rename(columns={'trans_date_trans_time': 'transaction_date_time'})
        df = df.drop(columns=['zip', 'state'])

        # Time formatting
        df['transaction_date_time'] = pd.to_datetime(df['transaction_date_time'])
        df['transaction_date'] = df['transaction_date_time'].dt.date
        df['transaction_day'] = df['transaction_date_time'].dt.day_name()
        df['transaction_time'] = df['transaction_date_time'].dt.time
        df['dob'] = pd.to_datetime(df['dob'])

        df['resident name'] = df['first'] + ' ' + df['last']
        df = df.drop(columns=['first', 'last'])
        columns = list(df.columns)
        resident_name_index = columns.index('resident name')
        columns.remove('resident name')
        columns.insert(columns.index('gender'), 'resident name') # insert before gender
        df = df[columns]

        # Time-based features
        df['hour'] = pd.to_datetime(df['transaction_time'], format='%H:%M:%S').dt.hour
        df['is_weekend'] = df['transaction_day'].apply(lambda x: 1 if x in ['Saturday', 'Sunday'] else 0)

        # distance based features
        df['distance_from_transaction'] = df.apply(
            lambda row: haversine((row['lat'], row['long']), (row['transaction_latitude'], row['transaction_longitude'])), axis=1
        )

        # age based features
        df['age'] = pd.to_datetime('today').year - pd.to_datetime(df['dob']).dt.year
        df['is_senior'] = df['age'].apply(lambda x: 1 if x >= 65 else 0)

        # drop dob
        df = df.drop(columns=['dob'])

        scaler_cols = ['amt', 'city_pop']
        df[scaler_cols] = standard_scaler.transform(df[scaler_cols])

        high_cardinality_cols = ['merchant', 'resident name', 'street', 'city', 'job', 'bank_account_num']
        # apply target encoding
        df[high_cardinality_cols] = target_encoder.transform(df[high_cardinality_cols])

        # apply onehot encoding
        low_cardinality_cols = ['gender', 'category', 'transaction_day']
        encoded_data = onehot_encoder.transform(df[low_cardinality_cols])
        encoded_df = pd.DataFrame(encoded_data, columns=onehot_encoder.get_feature_names_out(low_cardinality_cols))
        df = df.drop(columns=low_cardinality_cols).reset_index(drop=True)
        df = pd.concat([df, encoded_df], axis=1)

        # drop problematic columns for global model
        # drop_cols = ['transaction_date_time', 'trans_num', 'transaction_date', 'transaction_time', 'unix_time']
        drop_cols = ['transaction_date_time', 'trans_num', 'transaction_date', 'transaction_time']


        # data for global model
        df_global = df.copy()

        # drop problematic columns
        df_global = df_global.drop(columns=drop_cols, errors='ignore')

        # ensure remaining columns are of the correct type
        # make predictions
        predictions_proba = model.predict_proba(df_global)[:, 1]
        predictions = model.predict(df_global)

        logging.info("Predictions successful")

        # Format the response
        result = {
            "predictions": predictions.tolist(),
            "predictions_proba": predictions_proba.tolist()
        }
        return result["predictions_proba"]
    except Exception as e:
        logging.error("Error in prediction: ", str(e))
        return json.dumps({"error": str(e)})









def calculate_average(value1: float, value2: float) -> float:
    return (value1 + value2) / 2

def make_decision(value1: float, value2: float) -> int:
    average = calculate_average(value1, value2)
    return 0 if average < 0.5 else 1


@app.route(route="predict")
def predict(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')
    req_body = req.get_json()
    logging.info("req_body: %s", req_body)
    first_item = req_body[0]


    first_item.pop('EventProcessedUtcTime', None)
    first_item.pop('PartitionId', None)
    first_item.pop('EventEnqueuedUtcTime', None)
    email_address = first_item.pop('email_address', None)
    logging.info("email_address: %s", email_address)
    logging.info("first_item: %s", first_item)

    global_url = 'https://ml-paynet-ltyiq.eastus2.inference.ml.azure.com/score'
    # get raw data
    # call score endpoint global model
    result_global = score(first_item) #int

    # call score endpoint personal model
    result_personal = score(first_item) # dont forget to change here
    # create decision

    result_all = make_decision(result_global, result_personal)
    result_dict = {
        "is_fraud": result_all,
        "email_address": email_address 

    }
    asyncio.run(run(result_dict))

    # return decision to stream analytics?
  
    return func.HttpResponse(
            json.dumps(result_dict),
            status_code=200
    )
