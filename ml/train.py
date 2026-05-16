from scripts.config import DATABASE_URL
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.layers import (
    Input,
    LSTM,
    Dense,
    Dropout,
    BatchNormalization,
    Bidirectional,
)
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam
from sklearn.metrics import mean_absolute_error, mean_squared_error
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint

##1 conn setup
CONNECTION_STRING = DATABASE_URL
engine = create_engine(
    CONNECTION_STRING, connect_args={"sslmode": "require"}
)  ##ssl is required bcoz supbase accepts only encrypted conn
with engine.connect() as conn:
    result = conn.execute(text("SELECT current_database(), current_user, now()"))
    row = result.fetchone()
    print(f"Connected to: {row[0]} as {row[1]}")

##2 load the data
QUERY = """
SELECT
    symbol,
    trade_date,
    split,
    -- LSTM input features
    open_price,
    high_price,
    low_price,
    close_price,
    vwap,
    volume,
    log_return,
    daily_return,
    overnight_gap,
    intraday_range_pct,
    delivery_ratio,
    volume_zscore_20d,
    rsi_14,
    macd_line,
    macd_signal,
    macd_histogram,
    ema_20,
    ema_50,
    ema_200,
    bb_pct_b,
    price_above_ema20,
    price_above_ema50,
    price_above_ema200,
    golden_cross_20_50,
    news_article_count,
    -- regression target
    target_close
FROM raw_marts.mart_lstm_training
ORDER BY symbol, trade_date
"""

df = pd.read_sql(QUERY, engine)
df["trade_date"] = pd.to_datetime(df["trade_date"])
print(df)

print(f"rows: {len(df)}")
print(f"Symbols: {df['symbol'].nunique()}")
print(df["split"].value_counts())

##3 Data Quality Check + Fill Nulls: Find any missing values and fix them before they break the model.
##Neural networks cannot handle NaN. A single null in your input data causes the entire forward pass to return NaN — your loss becomes NaN,
##gradients become NaN, training breaks silently. So you must find and fill all nulls before going further.

null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=True)

print(
    null_pct[null_pct > 0]
)  ##Filters the calculated percentages so you only see the columns that have at least one missing value.

##define your feature columns (inputs to the model) and target column (what you're predicting):
FEATURE_COLS = [
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "vwap",
    "volume",
    "log_return",
    "daily_return",
    "overnight_gap",
    "intraday_range_pct",
    "delivery_ratio",
    "volume_zscore_20d",
    "rsi_14",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    "ema_20",
    "ema_50",
    "ema_200",
    "bb_pct_b",
    "price_above_ema20",
    "price_above_ema50",
    "price_above_ema200",
    "golden_cross_20_50",
    "news_article_count",
]
TARGET_COL = "target_close"

##Fill nulls per symbol (forward fill, then backward fill, then zero for anything left):
##groupby('symbol'): Separates your data into independent groups based on the stock ticker or symbol. This prevents data from one stock leaking into another.
##ffill() (Forward Fill): Looks at the group chronologically and carries the last known valid value forward to replace subsequent NaNs.
##bfill() (Backward Fill): Looks backward and carries the next known valid value backward to replace any NaNs at the very start of the group.
##transform(...): Ensures the filled values are mapped back to match the original structure and size of your DataFrame.
##lambda: The keyword that tells Python a small function is starting.
##g: The input variable. In your code, g represents each sub-DataFrame (group) created by groupby('symbol').
##:: The separator between the input and the action.
##in place of lambda we can do like this
# def fill_gaps(g):
#     return g.ffill().bfill()
# and use it like this
# df.groupby('symbol')[FEATURE_COLS].transform(fill_gaps)
##Step 1: Forward and Backward Fill by Symbol
df[FEATURE_COLS] = df.groupby("symbol")[FEATURE_COLS].transform(
    lambda g: g.ffill().bfill()
)


##Step 2: Global Zero Fill
df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0)

##4 Create the Classification Label: Turn target_close (a price) into a direction label — UP, DOWN, or NEUTRAL.
THRESHOLD = 0.05  ##5%
##Calculate the actual return: (tomorrow - today) / today
df["return_for_label"] = (df["target_close"] - df["close_price"]) / (df["close_price"])
### Assign 0 = DOWN, 1 = NEUTRAL, 2 = UP
df["direction_label"] = np.where(
    df["return_for_label"] > THRESHOLD,
    2,  # UP
    np.where(df["return_for_label"] > -THRESHOLD, 0, 1),  # DOWN  # NEUTRAL
)
## check the balance:
label_map = {0: "DOWN", 1: "NEUTRAL", 2: "UP"}
print(df["direction_label"].value_counts().rename(label_map))


##5 Split and Scale: Separate train/val/test, then scale all features to 0–1.
##You must split first, then scale. If you scale first on the whole dataset,
##the scaler has already "seen" the future prices when fitting — leakage. The correct order is: split → fit scaler on train → transform all three splits.

##df['split'] == 'train': Scans the entire split column and
##creates a checklist of True/False values matching rows labeled 'train'.df[...]: Subsets the master DataFrame, extracting only the rows where that checklist is True.
##Memory Isolation: The .copy() function creates a brand new, independent DataFrame object in your computer's memory.
##Without .copy(), modifying train_df later would trigger Python's infamous SettingWithCopyWarning because Python wouldn't know if you meant to change the original df too.
train_df = df[df["split"] == "train"].copy()
val_df = df[df["split"] == "val"].copy()
test_df = df[df["split"] == "test"].copy()
##Fit the feature scaler on training data only, then apply to all:
feature_scaler = MinMaxScaler(feature_range=(0, 1))
feature_scaler.fit(train_df[FEATURE_COLS])  # learn min/max from training data only
##now Apply to all three splits
for split_df in [train_df, val_df, test_df]:
    split_df[FEATURE_COLS] = feature_scaler.transform(split_df[FEATURE_COLS])
##Separate scaler for the target
target_scaler = MinMaxScaler(feature_range=(0, 1))
target_scaler.fit(train_df[["target_close"]])

##6 Build Sequences
##Convert rows of daily data into 3D arrays the LSTM can learn from.
##LSTM doesn't read one row at a time. It reads a sequence — "here are the last 60 days, now predict day 61."
##You need to slide a window over the data and package each window as one training sample.
##The final shape is (N, 60, 25) — N samples, each is a 60-day sequence, each day has 25 features.
##Flow:
##  1) takes input as split_df which is our test/train.val datasets, and takes sequnce lenght
## 2) returns : input sequences, regression targets, classification targets, metadata
## 3) Initialize empty containers :X_list(Input sequences), y_reg_list(Regression target), y_cls_list(Classification target), meta_list(Classification target) = [], [], [], []
## 4) Process each stock separately : for symbol, grp in split_df.groupby("symbol"): Without this, sequences could accidentally mix: RELIANCE day 59, RELIANCE day 60, TCS day 1 , this is nonsense
## 5)Sort by date: grp = grp.sort_values("trade_date").reset_index(drop=True) (Ensures chronological order, LSTMs require time order.)
## why step 5 bcoz jan 5, jan 2, jan 9 : bad -> jan 2, jan 5, jan 9 : good
## 6) Extract feature matrix: features = grp[FEATURE_COLS].values
## 7) Extract targets: targets_reg = grp["target_close_scaled"].values (This is what your regression head predicts.)
## 8) Classification target :targets_cls = grp["direction_label"].values
## 9) extract dates
## 10) Skip stocks with insufficient history
## 11) Sliding window creation: this si the heart : for i in range(seq_len, len(grp)):


SEQ_LEN = 60


def build_sequences(split_df, seq_len=SEQ_LEN):
    ##Initialize empty containers
    X_list, y_reg_list, y_cls_list, meta_list = [], [], [], []
    ##Process each stock separately
    for symbol, grp in split_df.groupby("symbol"):
        ##Sort by date
        grp = grp.sort_values("trade_date").reset_index(drop=True)
        ##Extract feature matrix
        features = grp[FEATURE_COLS].values
        ##Extract targets
        targets_reg = grp["target_close_scaled"].values
        ##Classification target
        targets_cls = grp["direction_label"].values
        ## extract dates
        dates = grp["trade_date"].values
        ##Skip stocks with insufficient history
        if len(grp) <= seq_len:
            continue
        ##Sliding window creation
        for i in range(seq_len, len(grp)):
            ##Input sequence
            X_list.append(features[i - seq_len : i])  # 60 rows before i
            ##Regression target
            y_reg_list.append(targets_reg[i])  # what we want to predict
            ## Classification target
            y_cls_list.append(targets_cls[i])
            ##metadata
            meta_list.append({"symbol": symbol, "trade_date": dates[i]})
    ##Convert lists to NumPy arrays
    ##X
    X = np.array(X_list, dtype=np.float32)  # (N, 60, 25)
    ##reg target
    y_reg = np.array(y_reg_list, dtype=np.float32)  # (N,)
    ##classification target
    y_cls = np.array(y_cls_list, dtype=np.int32)  # (N,)
    ##metadata df
    meta = pd.DataFrame(meta_list)

    return X, y_reg, y_cls, meta


X_train, y_reg_train, y_cls_train, meta_train = build_sequences(train_df)
X_val, y_reg_val, y_cls_val, meta_val = build_sequences(val_df)
X_test, y_reg_test, y_cls_test, meta_test = build_sequences(test_df)
print(X_train.shape)  # should be (N, 60, 25)


##7 Build the Regression Model
##building a pipeline: input enters at the top, passes through layers that transform it,
## and a single number comes out the bottom. Each layer learns different patterns — the first LSTM layer might learn short-term momentum, the second might learn longer trends.
##Dropout randomly switches off neurons during training so the model doesn't memorise the training data.


def build_regression_model(seq_len, n_features):
    ## ------------------------------------------------------------
    ## BUILDING A REGRESSION LSTM PIPELINE
    ##
    ## Input enters at the top
    ## passes through multiple transformation layers
    ## each layer learns patterns from stock history
    ## final output = predicted next-day close price
    ##
    ## Input Shape Example:
    ## (60, 25)
    ## 60 = past 60 trading days
    ## 25 = features per day
    ## ------------------------------------------------------------

    ## 1) INPUT LAYER
    ## takes the sequence data we created earlier
    ## shape = (seq_len, n_features)
    ## Example:
    ## (60, 25)
    ## means:
    ## 60 timesteps (days)
    ## 25 features for each day
    inp = Input(shape=(seq_len, n_features))
    ## 2) FIRST LSTM LAYER
    ## This is the first memory-processing layer
    ## LSTM(128)
    ## = 128 memory units / neurons
    ## Each neuron learns different temporal patterns like:
    ## - short-term momentum
    ## - volume spikes
    ## - moving average behavior
    ## - RSI reversals
    ## return_sequences=True is VERY IMPORTANT
    ## Without it:
    ## layer outputs only final hidden state
    ## With it:
    ## layer outputs all timesteps
    ## Input:
    ## (batch, 60, 25)
    ## Output:
    ## (batch, 60, 128)
    ## Why?
    ## Because next LSTM layer needs the full sequence
    x = LSTM(128, return_sequences=True)(inp)
    ## 3) BATCH NORMALIZATION
    ## stabilizes training
    ## It normalizes activations
    ## Why needed?
    ## Neural networks can become unstable when values
    ## shift too much during training
    ## BatchNorm keeps values in a controlled range
    ## Benefits:
    ## - faster convergence
    ## - smoother training
    ## - less sensitivity to initialization
    x = BatchNormalization()(x)
    ## 4) DROPOUT
    ## randomly disables 20% neurons during training
    ## Prevents overfitting
    ## Why?
    ## If model relies too heavily on specific neurons,
    ## it may memorize training data
    ## Dropout forces model to learn robust patterns
    x = Dropout(0.2)(x)
    ## 5) SECOND LSTM LAYER
    ## deeper temporal understanding layer
    ## First LSTM:
    ## learns local / short-term patterns
    ## Second LSTM:
    ## combines them into higher-level trends
    ## LSTM(64)
    ## = 64 memory units
    ## return_sequences=False
    ## VERY IMPORTANT
    ## This means:
    ## only final hidden state is returned
    ## Why?
    ## We only need ONE summary representation
    ## for predicting the next close price
    ## Input:
    ## (batch, 60, 128)
    ## Output:
    ## (batch, 64)
    x = LSTM(64, return_sequences=False)(x)
    ## 6) BATCH NORMALIZATION AGAIN
    ## stabilizes compressed temporal representation
    ## Helps deeper network train more smoothly
    x = BatchNormalization()(x)
    ## 7) DROPOUT AGAIN
    ## another regularization layer
    ## reduces overfitting risk
    x = Dropout(0.2)(x)
    ## 8) DENSE LAYER
    ## fully connected layer
    ## takes temporal summary from LSTM
    ## and learns nonlinear combinations
    ## Dense(32)
    ## = 32 neurons
    ## activation='relu'
    ## ReLU:
    ## f(x) = max(0, x)
    ## Why ReLU?
    ## introduces non-linearity
    ## allows model to learn complex relationships
    x = Dense(32, activation="relu")(x)
    ## 9) SMALL DROPOUT
    ## 10% dropout
    ## small because we're close to output layer
    ## too much dropout here could weaken prediction
    x = Dropout(0.1)(x)
    ## 10) OUTPUT LAYER
    ## Dense(1)
    ## outputs ONE value
    ## this is predicted next-day close price
    ## activation='linear'
    ## linear means:
    ## output can be any real number
    ## Why linear?
    ## because this is regression
    ## Example output:
    ## 0.842
    out = Dense(1, activation="linear")(x)
    ## 11) BUILD MODEL
    ## connects input -> all layers -> output
    model = Model(inputs=inp, outputs=out)
    ## 12) COMPILE MODEL
    model.compile(
        ## Adam optimizer
        ## adaptive learning rate optimizer
        ## very common default for deep learning
        optimizer=Adam(learning_rate=0.001),
        ## Loss = Mean Squared Error
        ## Formula:
        ## MSE = average((actual - predicted)^2)
        ## Why?
        ## penalizes large mistakes heavily
        loss="mse",
        ## Metric = Mean Absolute Error
        ## Formula:
        ## MAE = average(|actual - predicted|)
        ## easier to interpret than MSE
        metrics=["mae"],
    )
    ## 13) RETURN MODEL
    return model


## build model using:
## 60-day sequence
## number of selected features
reg_model = build_regression_model(SEQ_LEN, len(FEATURE_COLS))
## print architecture summary
reg_model.summary()


##8 Train the Regression Model
##Training is a loop: feed a batch of samples → model makes predictions → compare to actual → calculate loss → backpropagate error → update weights.
##You let this run for up to 100 epochs, but EarlyStopping will cut it off if the validation loss stops improving for 10 epochs in a row.
##reg_history stores the loss/MAE at every epoch — you'll use this to plot the training curves
reg_callbacks = [
    EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True),
    ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=5, min_lr=1e-6),
    ModelCheckpoint(
        "/content/best_regression.keras", monitor="val_loss", save_best_only=True
    ),
]

reg_history = reg_model.fit(
    X_train,
    y_reg_train,  # training data
    validation_data=(X_val, y_reg_val),  # validation data — model never trains on this
    epochs=100,
    batch_size=256,  # process 256 samples at a time
    callbacks=reg_callbacks,
)

##9Evaluate Regression:
##During training the model saw train and val. Test is untouched. You run predictions on test, undo the scaling to get back to ₹,
## and calculate how wrong the model is on average. MAPE (mean absolute percentage error) is the most human-readable — "the model is off by X% on average."
y_pred_scaled = reg_model.predict(X_test).flatten()

# Undo scaling — convert 0-1 predictions back to ₹
y_pred_inr = target_scaler.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()
y_true_inr = target_scaler.inverse_transform(y_reg_test.reshape(-1, 1)).flatten()

mae = mean_absolute_error(y_true_inr, y_pred_inr)
rmse = np.sqrt(mean_squared_error(y_true_inr, y_pred_inr))
mape = np.mean(np.abs((y_true_inr - y_pred_inr) / y_true_inr)) * 100

print(f"MAE  : ₹{mae:.2f}")  # average error in rupees
print(f"RMSE : ₹{rmse:.2f}")  # penalises large errors more
print(f"MAPE : {mape:.2f}%")  # percentage — most intuitive
