import pandas as pd

def mean_feature(df: pd.DataFrame, rolling_window):
    # Calculate mean and add prefix
    df_mean = df.rolling(rolling_window).mean()
    prefix1 = '_mean_' + rolling_window
    df_mean.name = df_mean.name + prefix1

    return df_mean

def min_feature(df: pd.DataFrame, rolling_window):
    # Calculate mean and add prefix
    df_min = df.rolling(rolling_window).min()
    prefix1 = '_min_' + rolling_window
    df_min.name = df_min.name + prefix1

    return df_min

def max_feature(df: pd.DataFrame, rolling_window):
    # Calculate mean and add prefix
    df_max = df.rolling(rolling_window).mean()
    prefix1 = '_max_' + rolling_window
    df_max.name = df_max.name + prefix1

    return df_max

def std_feature(df: pd.DataFrame, rolling_window):
    # Calculate mean and add prefix
    df_std = df.rolling(rolling_window).mean()
    prefix1 = '_std_' + rolling_window
    df_std.name = df_std.name + prefix1

    return df_std

def autocorr_feature(df: pd.DataFrame, rolling_window):
    # Calculate mean and add prefix
    df_autocorr = df.rolling(rolling_window).mean()
    prefix1 = '_autocorr_' + rolling_window
    df_autocorr.name = df_autocorr.name + prefix1

    return df_autocorr

feature_list = {
    "mean" : mean_feature,
    "min" : min_feature,
    "max" : max_feature,
    "autocorr" : autocorr_feature,
    "std" : std_feature
}