import pandas as pd
import numpy as np
from numpy import float32
import matplotlib as mpl
import matplotlib.pyplot as plt

BUCKET_VOLUME_SIZE = 844  # to be tweated
WINDOWS_LENGTH = 50  # to be tweated

sum_v_tau_b_minus_s = 0
v_tau_b_mius_s = [
    0
] * WINDOWS_LENGTH  # a list to save values of |v_tau_s - v_tau_b|
vpin_num = 0

vpin_df = pd.DataFrame(columns=['bucket_time', 'vpin'])


def new_bucket(buy_volume, sell_volume, bucket_time):
    global sum_v_tau_b_minus_s, v_tau_b_mius_s, vpin_num, vpin_df

    sum_v_tau_b_minus_s = sum_v_tau_b_minus_s - v_tau_b_mius_s[
        vpin_num % WINDOWS_LENGTH] + abs(buy_volume - sell_volume)
    v_tau_b_mius_s[vpin_num % WINDOWS_LENGTH] = abs(buy_volume - sell_volume)
    vpin_num += 1
    if vpin_num >= WINDOWS_LENGTH:
        vpin = sum_v_tau_b_minus_s / (WINDOWS_LENGTH * BUCKET_VOLUME_SIZE)
        # cumulative_count_vpin = len(vpin_df[vpin >= vpin_df['vpin']])
        # len_vpin = len(vpin_df)
        vpin_df = vpin_df.append(  #  add a new timeseries of VPIN (bucket_time, vpin)
            {
                'bucket_time': bucket_time,
                'vpin': vpin
                # 'cdf_vpin': cumulative_count_vpin / len_vpin
            },
            ignore_index=True)


# all_trades = pd.read_csv('./BTCUSDT/BINANCE_BTCUSDT_1808.csv')
all_trades = pd.read_csv('./BTCUSDT/binance_20180801.csv')
v_tau = 0
v_tau_b = 0
v_tau_s = 0
todo_volume = 0

data_num = len(all_trades)
data_i = 0

# special process for the first row of data
last_price = 0
if all_trades.iloc[0]["buy_or_sell"] == "s":
    last_price = float('inf')

for trade in all_trades.itertuples():
    todo_volume = getattr(trade, "amount")
    todo_timestamp = getattr(trade, "time")

    while v_tau + todo_volume > BUCKET_VOLUME_SIZE:  # if current bucket is full
        delta = BUCKET_VOLUME_SIZE - v_tau
        v_tau += delta  # v_tau -> V (bucket size)
        if getattr(trade, "price") > last_price:
            v_tau_b += delta
        elif getattr(trade, "price") < last_price:
            v_tau_s += delta
        else:
            v_tau_b += (delta / 2)
            v_tau_s += (delta / 2)

        v_timestamp = todo_timestamp
        new_bucket(v_tau_b, v_tau_s, todo_timestamp
                   )  # generate new bucket: 1. new element 2. update VPIN

        todo_volume -= delta
        v_tau = 0
        v_tau_b = 0
        v_tau_s = 0

    v_tau += todo_volume
    if getattr(trade, "price") > last_price:
        v_tau_b += todo_volume
    elif getattr(trade, "price") < last_price:
        v_tau_s += todo_volume
    else:
        v_tau_b += (todo_volume / 2)
        v_tau_s += (todo_volume / 2)

    last_price = getattr(trade, "price")
    data_i += 1
    print(
        "Processed Data: ",
        round(data_i / data_num * 100, 2),
        "%",
        sep="",
        end="\r")

# plot
all_trades['time'] = pd.to_datetime(all_trades['time'])
all_trades.set_index('time')

vpin_df['bucket_time'] = pd.to_datetime(vpin_df['bucket_time'])
vpin_df.set_index('bucket_time')

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

ax1.plot(all_trades['time'], all_trades['price'], 'b-')
ax2.plot(vpin_df['bucket_time'], vpin_df['vpin'], 'g-')
plt.show()