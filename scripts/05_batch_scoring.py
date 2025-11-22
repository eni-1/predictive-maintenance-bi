import os
import pandas as pd
import joblib
from sqlalchemy import create_engine
from dotenv import load_dotenv

# setup
load_dotenv()
db_url = os.getenv("DATABASE_URL")

if not db_url:
    print("DATABASE_URL not found.")
    exit()

# connect to database
try:
    engine = create_engine(db_url)
    print("Connected to DB.")
except Exception as e:
    print("DB Connection Failed:", e)
    exit()

# load models
print("Loading Models...")
try:
    model_xgb = joblib.load('models/xgb_classifier.joblib')
    model_rf = joblib.load('models/rf_regressor.joblib')
    le = joblib.load('models/label_encoder.joblib')
    print("Models loaded.")
except FileNotFoundError:
    print("Models not found.")
    exit()

# fetch data
print("Fetching data...")
query = 'SELECT * FROM "ANALYTICAL_MAINTENANCE" ORDER BY timestamp_clean ASC'
df = pd.read_sql(query, engine)

# preprocess data
ids = df[['udi', 'product_id', 'timestamp_clean']]

# encoding function
def safe_encode(value):
    if value in le.classes_:
        return list(le.classes_).index(value)
    else:
        return 0

# prepare feature sets
# xgboost needs process_temperature, random forest does not
# xgboost predicts failure, random forest predicts process_temperature

# set1: xgboost for failure prediction
cols_to_drop_xgb = ['udi', 'product_id', 'timestamp_clean', 'machine_failure', 'operational_status']
features_xgb = df.drop(columns=cols_to_drop_xgb)
features_xgb['machine_type'] = features_xgb['machine_type'].map(safe_encode)


# set2: random forest for process temperature prediction
cols_to_drop_rf = cols_to_drop_xgb + ['process_temperature']
features_rf = df.drop(columns=cols_to_drop_rf)
features_rf['machine_type'] = features_rf['machine_type'].map(safe_encode)

# predictions
print("Running Failure Prediction (XGBoost)...")
probs = model_xgb.predict_proba(features_xgb)[:, 1]

print("Running Virtual Sensor (Random Forest)...")
pred_temp = model_rf.predict(features_rf)

# combined results
results = ids.copy()
results['failure_probability'] = probs
results['risk_label'] = results['failure_probability'].apply(lambda x: 'High Risk' if x > 0.5 else 'Normal')

results['predicted_process_temp'] = pred_temp
results['actual_process_temp'] = df['process_temperature']
results['temp_anomaly_score'] = abs(results['actual_process_temp'] - results['predicted_process_temp'])

# upload to DB
table_name = 'predictions'
print(f"Uploading {len(results)} predictions to table '{table_name}'...")

try:
    results.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Predictions uploaded")
except Exception as e:
    print("Upload Failed:", e)