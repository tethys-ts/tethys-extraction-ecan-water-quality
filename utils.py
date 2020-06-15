"""

"""
import os
import io
import zstandard as zstd
import pickle
import pandas as pd
import numpy as np

#####################################################
### Functions


def list_parse_s3(s3_client, bucket, prefix, default_date='1900-01-01', date_type='date', local_tz=None, start_after='', delimiter='', continuation_token=''):
    """

    """

    js = []
    while True:
        js1 = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, StartAfter=start_after, Delimiter=delimiter, ContinuationToken=continuation_token)

        if 'Contents' in js1:
            js.extend(js1['Contents'])
            if 'NextContinuationToken' in js1:
                continuation_token = js1['NextContinuationToken'].replace('%3B', ';')
            else:
                break
        else:
            break

    if 'Contents' in js1:
        f_df1 = pd.DataFrame(js)
        f_df1['run_date'] = pd.to_datetime(f_df1.Key.str.findall('\d\d\d\d\d\d\d\dT\d\d\d\d\d\dZ').apply(lambda x: x[0]), utc=True, errors='coerce')
        last_run_date = f_df1.run_date.max().tz_convert(local_tz).tz_localize(None)
    else:
        f_df1 = pd.DataFrame()
        last_run_date = pd.Timestamp(default_date)

    if date_type == 'str':
        last_run_date = last_run_date.strftime('%Y-%m-%d %H:%M:%S')

    return f_df1, last_run_date


def write_pkl_zstd(obj, file_path=None):
    """
    Serializer using pickle and zstandard. Converts any object that can be pickled to a binary object then compresses it using zstandard. Optionally saves the object to disk.

    Parameters
    ----------
    obj : any
        Any pickleable object.
    file_path : None or str
        Either None to return the bytes object or a str path to save it to disk.

    Returns
    -------
    If file_path is None, then it returns the byte object, else None.
    """
    p_obj = pickle.dumps(obj, protocol=5)
    cctx = zstd.ZstdCompressor(level=1)
    c_obj = cctx.compress(p_obj)

    if isinstance(file_path, str):
        with open(file_path, 'wb') as p:
            p.write(c_obj)
    else:
        return c_obj


def read_pkl_zstd(obj):
    """
    Deserializer from a pickled object compressed with zstandard.

    Parameters
    ----------
    obj : bytes or str
        Either a bytes object that has been pickled and compressed or a str path to the file object.

    Returns
    -------
    Python object
    """
    dctx = zstd.ZstdDecompressor()
    if isinstance(obj, str):
        with open(obj, 'rb') as p:
            p1 = pickle.loads(dctx.decompress(p.read()))
    elif isinstance(obj, bytes):
        p1 = pickle.loads(dctx.decompress(obj))
    else:
        raise TypeError('obj must either be a str path or a bytes object')

    return p1


def grp_ts_agg(df, grp_col, ts_col, freq_code, discrete=False, **kwargs):
    """
    Simple function to aggregate time series with dataframes with a single column of sites and a column of times.

    Parameters
    ----------
    df : DataFrame
        Dataframe with a datetime column.
    grp_col : str or list of str
        Column name that contains the sites.
    ts_col : str
        The column name of the datetime column.
    freq_code : str
        The pandas frequency code for the aggregation (e.g. 'M', 'A-JUN').
    discrete : bool
        Is the data discrete? Will use proper resampling using linear interpolation.

    Returns
    -------
    Pandas resample object
    """

    df1 = df.copy()
    if type(df[ts_col].iloc[0]) is pd.Timestamp:
        df1.set_index(ts_col, inplace=True)
        if isinstance(grp_col, str):
            grp_col = [grp_col]
        else:
            grp_col = grp_col[:]
        if discrete:
            val_cols = [c for c in df1.columns if c not in grp_col]
            df1[val_cols] = (df1[val_cols] + df1[val_cols].shift(-1))/2
        grp_col.extend([pd.Grouper(freq=freq_code, **kwargs)])
        df_grp = df1.groupby(grp_col)
        return (df_grp)
    else:
        print('Make one column a timeseries!')


def compare_dfs(old_df, new_df, on):
    """
    Function to compare two DataFrames with nans and return a dict with rows that have changed (diff), rows that exist in new_df but not in old_df (new), and rows  that exist in old_df but not in new_df (remove).
    Both DataFrame must have the same columns.

    Parameters
    ----------
    old_df : DataFrame
        The old DataFrame.
    new_df : DataFrame
        The new DataFrame.
    on : str or list of str
        The primary key(s) to index/merge the two DataFrames.

    Returns
    -------
    dict of DataFrames
        As described above, keys of 'diff', 'new', and 'remove'.
    """
    if ~np.in1d(old_df.columns, new_df.columns).any():
        raise ValueError('Both DataFrames must have the same columns')

    val_cols = [c for c in old_df.columns if not c in on]
    all_cols = old_df.columns.tolist()

    comp1 = pd.merge(old_df, new_df, on=on, how='outer', indicator=True, suffixes=('_x', ''))

    rem1 = comp1.loc[comp1._merge == 'left_only', on].copy()
    add1 = comp1.loc[comp1._merge == 'right_only', all_cols].copy()
    comp2 = comp1[comp1._merge == 'both'].drop('_merge', axis=1).copy()
#    comp2[comp2.isnull()] = np.nan

    old_cols = on.copy()
    old_cols_map = {c: c[:-2] for c in comp2 if '_x' in c}
    old_cols.extend(old_cols_map.keys())
    old_set = comp2[old_cols].copy()
    old_set.rename(columns=old_cols_map, inplace=True)
    new_set = comp2[all_cols].copy()

    comp_list = []
    for c in val_cols:
        isnull1 = new_set[c].isnull()
        if isnull1.any():
            new_set.loc[new_set[c].isnull(), c] = np.nan
        if old_set[c].dtype.name == 'float64':
            c1 = ~np.isclose(old_set[c], new_set[c])
        elif old_set[c].dtype.name == 'object':
            new_set[c] = new_set[c].astype(str)
            c1 = old_set[c].astype(str) != new_set[c]
        elif old_set[c].dtype.name == 'geometry':
            old1 = old_set[c].apply(lambda x: hash(x.wkt))
            new1 = new_set[c].apply(lambda x: hash(x.wkt))
            c1 = old1 != new1
        else:
            c1 = old_set[c] != new_set[c]
        notnan1 = old_set[c].notnull() | new_set[c].notnull()
        c2 = c1 & notnan1
        comp_list.append(c2)
    comp_index = pd.concat(comp_list, axis=1).any(1)
    diff_set = new_set[comp_index].copy()

    dict1 = {'diff': diff_set, 'new': add1, 'remove': rem1}

    return dict1



























