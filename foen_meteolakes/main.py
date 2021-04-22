import os, ftplib, sys, traceback, pysftp
from datetime import datetime, date, timedelta, timezone
import pandas as pd
import xml.etree.ElementTree as et


def log(str, indent=0, start=False):
    if start:
        out = "\n" + str
    else:
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + str
    print(out)
    with open("log_file.txt", "a") as file:
        file.write(out + "\n")


def error(str):
    out = datetime.now().strftime("%H:%M:%S.%f") + "   ERROR: " + str
    with open("log_hydrosim.txt", "a") as file:
        file.write(out + "\n")
    raise ValueError(str)


def parse_waterml(file):
    """
    Parses WaterML file to Pandas dataframe and attribute dictionary
    :param string file: File path.
    """
    wml2 = "{http://www.opengis.net/waterml/2.0}"
    gml = "{http://www.opengis.net/gml/3.2}"
    xtree = et.parse(file)
    xroot = xtree.getroot()

    # Parse Attributes
    attributes = {}
    attributes["location"] = xroot.find(gml + "name").text
    for i in xroot.iter(wml2 + "MeasurementTimeseries"):
        name = i.attrib[gml + "id"]
        attributes["name"] = name
        attributes["station"] = name.split("_")[1]
        attributes["parameter"] = name.split("_")[2]
    for i in xroot.iter(wml2 + "uom"):
        attributes["unit"] = i.attrib["code"]

    # Parse Data
    df_cols = ["timestring", "value"]
    rows = []
    for i in xroot.iter(wml2 + "MeasurementTVP"):
        rows.append({"timestring": i.find(wml2 + "time").text,
                     "value": i.find(wml2 + "value").text})
    df = pd.DataFrame(rows, columns=df_cols)
    df["datetime"] = pd.to_datetime(df["timestring"])
    del df['timestring']
    df = df.sort_values('datetime')
    return df, attributes


def parse_log(file):
    """
    Parses log file to Pandas dataframe and attribute dictionary
    :param string file: File path.
    """
    cols = ["date", "time"]
    attributes = {}
    with open(file, 'r') as f:
        data_line = 0
        for line in f:
            if not line.startswith("#"):
                break
            elif line.startswith("# Parameter"):
                line_arr = line.split("\t")
                cols.append('%s (%s)' % (line_arr[2], line_arr[3]))
                cols.append('%s Quality' % line_arr[2])
            else:
                line_arr = line.replace("# ","").replace("\n","").split("\t")
                attributes[line_arr[0]] = line_arr[1]
            data_line = data_line + 1
    cols.append("NaN")
    df = pd.read_csv(file, sep='\t', header=None, skiprows=data_line)
    df.columns = cols
    del df['NaN']
    df["datetime"] = pd.to_datetime(df["date"] + "T" + df["time"] + attributes["Timezone"].replace("+","-"))
    df = df.sort_values('datetime')
    # Check time zone issues
    return df, attributes


def parse_csv(file, parameter, resample=False):
    """
    Parses csv file to Pandas dataframe and attribute dictionary
    :param string file: File path.
    :param string parameter: Parameter name
    :param string resample: Resample period (optional)
    """
    log("Reading file: "+file)
    df = pd.read_csv(file, encoding="utf-8")
    df["datetime"] = pd.to_datetime(df["Time"])
    df = df.sort_values('datetime')
    df = df.drop(['Time'], axis=1)
    df.columns = [parameter, "datetime"]
    if resample:
        df = df.set_index("datetime", drop=True)
        df = df.resample(resample).mean()
        df = df.reset_index()
    return df


def write_to_meteolakes(df, name, id, columns, folder):

    df["Time"] = df["datetime"].apply(lambda x: x.strftime('%H:%M'))
    df["Date"] = df["datetime"].apply(lambda x: x.strftime('%d.%m.%Y'))
    min_date = df["datetime"].min()
    max_date = df["datetime"].max()
    df = df.drop(["datetime"], axis=1)
    start_date = datetime(min_date.year, min_date.month, min_date.day)
    end_date = datetime(max_date.year, max_date.month, max_date.day)
    for i in range((end_date - start_date).days + 1):
        dt = start_date + timedelta(days=i)
        filename = "BAFU" + str(id) + "_" + dt.strftime("%Y%m%d") + ".txt"
        file_path = os.path.join(folder, name, str(dt.year), filename)
        log("Writing file: " + file_path)
        if os.path.isfile(file_path):
            df_o = pd.read_csv(file_path, sep="\t")
            os.remove(file_path)
        else:
            df_o = pd.DataFrame([], columns=columns)
        df_o = df_o.append(df)
        df_o = df_o.loc[df_o['Date'] == dt.strftime('%d.%m.%Y')]
        df_o = df_o.reset_index(drop=True)
        df_o = df_o.drop_duplicates(subset=['Time'])
        df_o = df_o.sort_values(by='Time')
        df_o = df_o.fillna("NaN")
        df_o.to_csv(file_path, index=False, sep="\t", float_format='%.3f')



def append_new_data(station, file, new_data, time_col, value_col, resample="H", date_format="%Y.%m.%d %H:%M"):
    """
    Combines ASC file with pandas df of new data.
    :param string station: Station id.
    :param string file: ASC file path.
    :param pandas.core.frame.DataFrame new_data: Pandas dataframe that contains timeseries of values.
    :param string time_col: Name of time column in pandas dataframe.
    :param string value_col: Name of value column in pandas dataframe.
    :param string/False resample: False for no resampling, else rule - see pandas.Series.resample.
    :param string date_format: Date format for parsing and writing to output file.
    """
    if resample:
        new_data = new_data.resample(resample, on=time_col).median().reset_index()
    timedelta = (new_data[time_col]-new_data[time_col].shift()).mode()[0]
    new_data = new_data.sort_values(time_col)
    with open(file, 'r+', encoding='iso-8859-1') as f:
        for line in f:
            pass
        dt = datetime.strptime(line.split(";")[1].split("-")[1], date_format).replace(tzinfo=timezone.utc)
        for index, row in new_data.iterrows():
            time = row[time_col]
            value = row[value_col]
            time_str_s = datetime.strftime(time - timedelta, date_format)
            #time_str_s = datetime.strftime(time, date_format)
            time_str_e = datetime.strftime(time, date_format)
            if time > dt:
                if isinstance(value, int) or isinstance(value, float):
                    f.write(';'.join([station,time_str_s+'-'+time_str_e,'%8.3f' % value])+'\n')
                else:
                    print("File: " + file + ". Unable to add row with time: "+time_str_s+" and value: "+str(value))


def find_file(sftp, folder, stn, filt, pref):
    try:
        files = sftp.listdir(folder + "/" + stn)
    except:
        print('Folder %s: not a SFTP directory.' % folder)
        return False
    flow_files = []
    for file in files:
        if filt.lower() in file.lower():
            flow_files.append(file)
    if len(flow_files) == 0:
        return False
    elif len(flow_files) == 1:
        return folder + "/" + stn + "/" + flow_files[0]
    else:
        for p in pref:
            for f in flow_files:
                if p.lower() in f.lower():
                    return folder + "/" + stn + "/" + f
        return folder + "/" + stn + "/" + flow_files[0]


def download_file_sftp(sftp, remote_file, folder, overwrite=True):
    log("Downloading file: "+os.path.basename(remote_file))
    local_file = os.path.join(folder, os.path.basename(remote_file))
    if os.path.isfile(local_file) and overwrite:
        os.remove(local_file)
        log("Overwriting existing file: "+local_file, 1)

    try:
        sftp.get(remotepath=remote_file, localpath=local_file)
        log("Successfully downloaded file.")
        return local_file
    except:
        log('Cannot download %s' % remote_file)
        return False


def remove_file(path):
    if os.path.exists(path):
        os.remove(path)


def connect_sftp(server, user, ssh):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    return pysftp.Connection(host=server, username=user, private_key=ssh, cnopts=cnopts)


def process(server, user, ssh, inputdir, outputdir, tempdir):

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host=server, username=user, private_key=ssh, cnopts=cnopts)

    files = os.listdir(inputdir)
    files.sort()
    for file in files:
        if file[0] == 'T' or file[0] == 'Q':
            stn = file[2:6]
            if file[0] == 'T':
                remote_file = find_file(sftp, "CSV", stn, "temp", [])
            elif file[0] == 'Q':
                remote_file = find_file(sftp, "CSV", stn, "fluss", ["pneumatik", "schacht", "drucksonde"])
            if remote_file:
                path = download_file_sftp(sftp, remote_file, tempdir)
                if path:
                    df, attributes = parse_csv(path)
                    append_new_data(stn, os.path.join(inputdir, file), df, "datetime", attributes["parameter"])
                    remove_file(path)