import numpy as np
import pandas as pd


def haversine_distance(lng1, lat1, lng2, lat2):
    lat1, lng1, lat2, lng2 = map(np.radians, (lat1, lng1, lat2, lng2))
    AVG_EARTH_RADIUS = 6371 
    lat = lat2 - lat1
    lng = lng2 - lng1
    d = np.sin(lat * 0.5) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(lng * 0.5) ** 2
    h = 2 * AVG_EARTH_RADIUS * np.arcsin(np.sqrt(d))
    return h


def manhattan_distance(lng1, lat1, lng2, lat2):
    a = haversine_distance(lng1, lat1, lng2, lat1)
    b = haversine_distance(lng1, lat1, lng1, lat2)
    return a + b


def preprocess_taxi_data(X, kmeans_model=None):
    X_processed = X.copy()

    X_processed['pickup_datetime'] = pd.to_datetime(X_processed['pickup_datetime'])
    X_processed['pickup_hour'] = X_processed['pickup_datetime'].dt.hour
    X_processed['pickup_dayofweek'] = X_processed['pickup_datetime'].dt.dayofweek
    X_processed['pickup_month'] = X_processed['pickup_datetime'].dt.month

    X_processed["distance"] = manhattan_distance(
        X_processed['pickup_longitude'], X_processed['pickup_latitude'],
        X_processed['dropoff_longitude'], X_processed['dropoff_latitude']
    )

    if kmeans_model is not None:
        X_processed['pickup_cluster'] = kmeans_model.predict(X_processed[['pickup_latitude', 'pickup_longitude']])
        X_processed['dropoff_cluster'] = kmeans_model.predict(X_processed[['dropoff_latitude', 'dropoff_longitude']])

    if 'store_and_fwd_flag' in X_processed.columns:
        X_processed['store_and_fwd_flag'] = X_processed['store_and_fwd_flag'].map({'N': 0, 'Y': 1}).fillna(0).astype(int)
        
    drop_cols = ['id', 'pickup_datetime', 'dropoff_datetime', 'speed', 
                 'pu_lat_r', 'pu_lon_r', 'do_lat_r', 'do_lon_r', 'outliers']
    cols_to_drop = [col for col in drop_cols if col in X_processed.columns]
    X_processed.drop(columns=cols_to_drop, inplace=True)
    
    cat_cols = ['vendor_id', 'pickup_hour', 'pickup_dayofweek', 'pickup_month', 'pickup_cluster', 'dropoff_cluster']
    for col in cat_cols:
        if col in X_processed.columns:
            X_processed[col] = X_processed[col].astype(int)
            
    return X_processed