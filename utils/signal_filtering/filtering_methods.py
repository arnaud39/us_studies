from statsmodels.tsa.seasonal import seasonal_decompose
import statsmodels.api as sm
import numpy as np

hpfilter = sm.tsa.filters.hpfilter
seasonalfilter = lambda x, period=12: seasonal_decompose(
    x, period=period, model="multiplicative", extrapolate_trend="freq"
)

kernel = lambda R, c, h: (np.abs(R - c) <= h).astype(float) * (1 - np.abs(R - c) / h)
