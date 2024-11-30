import azure.functions as func
import logging
import urllib.request
import json
import os
import ssl
import ast

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


    global_url = 'https://ml-paynet-ltyiq.eastus2.inference.ml.azure.com/score'
    # get raw data
    # call score endpoint global model
    result_global = call_score_endpoint(global_url,first_item) #int

    # call score endpoint personal model
    result_personal = call_score_endpoint(global_url,first_item) # dont forget to change here
    # create decision

    result_all = make_decision(result_global, result_personal)

    # return decision to stream analytics?
  
    return func.HttpResponse(
            json.dumps(result_all),
            status_code=200
    )