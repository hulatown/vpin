import pandas as pd
import numpy as np
from numpy import float32
import matplotlib as mpl
import matplotlib.pyplot as plt
import math as math

BUCKET_VOLUME_SIZE = 146817  # to be tweated
WINDOWS_LENGTH = 25  # to be tweated

sum_v_tau_b_minus_s = 0
v_tau_b_mius_s = [
    0
] * WINDOWS_LENGTH  # a list to save values of |v_tau_s - v_tau_b|
vpin_num = 0

vpin_df = pd.DataFrame(columns=['bucket_time', 'vpin', 'cdf_vpin'])


def std(list):
    element = 0
    for item in list:
        element = element + float((item**2) / (len(list)))
    return math.sqrt(element)


def z(x):
    #'Cumulative distribution function for the standard normal distribution'
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def new_bucket(buy_volume, sell_volume, bucket_time):
    global sum_v_tau_b_minus_s, v_tau_b_mius_s, vpin_num, vpin_df

    sum_v_tau_b_minus_s = sum_v_tau_b_minus_s - v_tau_b_mius_s[
        vpin_num % WINDOWS_LENGTH] + abs(buy_volume - sell_volume)
    v_tau_b_mius_s[vpin_num % WINDOWS_LENGTH] = abs(buy_volume - sell_volume)
    vpin_num += 1
    if vpin_num >= WINDOWS_LENGTH:
        vpin = sum_v_tau_b_minus_s / (WINDOWS_LENGTH * BUCKET_VOLUME_SIZE)
        vpin_df = vpin_df.append(  #  add a new timeseries of VPIN (bucket_time, vpin)
            {
                'bucket_time': bucket_time,
                'vpin': vpin
            },
            ignore_index=True)


all_trades = pd.read_csv(
    './EOSUSDT/BINANCE_EOSUSDT_201901.csv', index_col='time', parse_dates=True)

resampled_trades = all_trades.resample('1min').agg({
    'price': 'ohlc',
    'amount': np.sum
})

v_tau = 0
v_tau_b = 0
v_tau_s = 0
v_i = 0

data_num = len(resampled_trades)
data_i = 0

resampled_trades['price_change'] = resampled_trades.apply(
    lambda x: x[3] - x[0], axis=1)

for trade in resampled_trades.iterrows():
    v_i = trade[1]['amount']['amount']
    delta_p_i = trade[1]['price_change']

    sigma_delta_p = std(
        resampled_trades['price_change']
        [0:data_i])  # calcute from the very beginning to the latest one
    v_tau += v_i
    v_tau_b_i = (v_i * z(delta_p_i / sigma_delta_p))
    v_tau_b += v_tau_b_i
    v_tau_s += (v_i - v_tau_b_i)

    if v_tau > BUCKET_VOLUME_SIZE:
        v_timestamp = trade[0]
        new_bucket(v_tau_b, v_tau_s, v_timestamp)
        v_tau = 0
        v_tau_b = 0
        v_tau_s = 0

    data_i += 1
    print(
        "Processed Data: ",
        round(data_i / data_num * 100, 2),
        "%",
        sep="",
        end="\r")

vpin_df_len = len(vpin_df)
vpin_df['cdf_vpin'] = vpin_df['vpin'].map(lambda x: len(vpin_df[x >= vpin_df[
    'vpin']]) / vpin_df_len)

# plot
vpin_df['bucket_time'] = pd.to_datetime(vpin_df['bucket_time'])
vpin_df.set_index('bucket_time')

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

ax1.plot(all_trades['price'], 'b-')
ax2.plot(vpin_df['bucket_time'], vpin_df['vpin'], 'g+--')
ax2.plot(vpin_df['bucket_time'], vpin_df['cdf_vpin'], 'r*--')
plt.show()
pass