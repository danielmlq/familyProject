import sys
from configobj import ConfigObj
from datetime import datetime
from datetime import timedelta
import math
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM
from keras.models import model_from_json
import pandas_datareader as web
import pandas_market_calendars as mcal

class LSTM_Model:
    def yfinance_data_loader(self, sticker, start_date, end_date):
        df = web.DataReader(sticker, data_source='yahoo', start=start_date, end=end_date)
        return df

    def truncate_raw_float_columns(self, input_df):
        input_df['Open'] = input_df.apply(lambda row: np.round(row.Open, 2), axis=1)
        input_df['High'] = input_df.apply(lambda row: np.round(row.High, 2), axis=1)
        input_df['Low'] = input_df.apply(lambda row: np.round(row.Low, 2), axis=1)
        input_df['Close'] = input_df.apply(lambda row: np.round(row.Close, 2), axis=1)

    def calculate_prediction(self, input_df, column_name):
        data = input_df.filter(column_name)
        dataset = data.values

        # Scale the data
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(dataset)

        # Size of dataset for training is 99% and test for 1%
        training_data_len = math.ceil(len(dataset) * .99)

        # Create the training dataset, the scaled training dataset, 99% of data used for training
        train_data = scaled_data[0: training_data_len, :]

        # Split the training data into x_train and y_train datasets
        x_train = []
        y_train = []
        for i in range(60, len(train_data)):
            x_train.append(train_data[i-60:i, 0])
            y_train.append(train_data[i, 0])
            if i <= 60:
                print(x_train)
                print(y_train)
                print()

        x_train, y_train = np.array(x_train), np.array(y_train)

        # Reshap the data
        x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))

        # Build the LSTM model
        lstm_model = Sequential()
        lstm_model.add(LSTM(50, return_sequences=True, input_shape=(x_train.shape[1], 1)))
        lstm_model.add(LSTM(50, return_sequences=False))
        lstm_model.add(Dense(25))
        lstm_model.add(Dense(1))
        lstm_model.compile(optimizer='adam', loss='mean_squared_error')

        # Train the model
        lstm_model.fit(x_train, y_train, batch_size=1, epochs=1)

        # Create the testing datasets
        test_data = scaled_data[training_data_len - 60: , :]

        # Create the test datasets x_test and y_test
        x_test = []
        y_test = dataset[training_data_len: , :]
        for i in range(60, len(test_data)):
            x_test.append(test_data[i-60:i, 0])

        # Convert the data to a np array
        x_test = np.array(x_test)

        # Reshape the data
        x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

        # Get the model predicted price values
        predictions = lstm_model.predict(x_test)
        predictions = scaler.inverse_transform(predictions)

        # Get the root mean squared error (RMSE)
        rmse = np.sqrt(np.mean(predictions - y_test)**2)

        print('rmse = {}'.format(rmse))

        # In the future, leverage valid dataframe to compare the actual value and prediction value
        # to calculate the gap to further reduce the error.
        valid = data[training_data_len:]
        valid['Predictions'] = predictions

        print(valid)

        print('Open = {}'.format(valid.iloc[-1]['Open']))
        print('Pred = {}'.format(valid.iloc[-1]['Predictions']))
        print('Date = {}'.format(valid.index[-1]))

        # Get the last 60 days of close price and convert to array from dataframe
        last_60_days_values = data[-60:].values
        # Scale the values in the array from 0 to 1
        last_60_days_scaled = scaler.transform(last_60_days_values)

        # Create an empty list
        prediction_test = []
        prediction_test.append(last_60_days_scaled)
        X_test = np.array(prediction_test)
        X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

        # Get prediction for scaled price
        pred_price = lstm_model.predict(X_test)
        pred_price = scaler.inverse_transform(pred_price)

        output_pred = np.round(np.float(pred_price[0][0]), 2)
        output_rmse = np.round(rmse, 2)

        print("output_pred: {}".format(output_pred))
        print("output_rmse: {}".format(output_rmse))

        file_name = '/Users/danielmeng/Desktop/spy_predict_test.json'
        weight_file = '/Users/danielmeng/Desktop/spy_predict_test.h5'
        json_file = lstm_model.to_json()
        with open(file_name, "w") as file:
            file.write(json_file)
        lstm_model.save_weights(weight_file)

if __name__ == "__main__":
    sticker = 'SPY'
    start_date = '2012-01-01'
    end_date = '2022-12-08'

    model = LSTM_Model()
    df = model.yfinance_data_loader(sticker, start_date, end_date)
    model.truncate_raw_float_columns(df)
    # col_names = ['Open', 'High', 'Low', 'Close']
    col_names = ['Open']

    # Get the last 60 days of close price and convert to array from dataframe
    df_values = df.filter(col_names).values
    print(df_values)
    # Scale the values in the array from 0 to 1
    scaler = MinMaxScaler(feature_range=(0, 1))
    values_scaled = scaler.fit_transform(df_values)

    # Create an empty list
    prediction_test2 = []
    prediction_test2.append(values_scaled)
    X_test = np.array(prediction_test2)
    X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

    file_name = '/Users/danielmeng/Desktop/spy_predict_test.json'
    weight_file = '/Users/danielmeng/Desktop/spy_predict_test.h5'
    # load json and create model
    file = open(file_name, 'r')
    model_json = file.read()
    file.close()
    loaded_model = model_from_json(model_json)
    # load weights
    loaded_model.load_weights(weight_file)

    # Get prediction for scaled price
    pred_price = loaded_model.predict(X_test)
    pred_price = scaler.inverse_transform(pred_price)

    output_pred = np.round(np.float(pred_price[0][0]), 2)

    print("output_pred: {}".format(output_pred))