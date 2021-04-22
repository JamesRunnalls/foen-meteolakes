import json
import os
import foen_meteolakes.main as fm

with open('stations.json', encoding='utf8') as f:
    stations = json.load(f)

with open('credentials.json', encoding='utf8') as f:
    credentials = json.load(f)

temp_folder = "temp"
out_folder = "Hydrodata"
os.remove("log_file.txt")
fm.log("------------STARTING NEW RUN------------", start=True)
server = fm.connect_sftp(credentials["server"], credentials["user"], credentials["ssh"])

for station in stations:
    for i in range(len(station["parameters"])):
        fm.download_file_sftp(server, station["parameters"][i]["file"], temp_folder)
        input_file = os.path.join(temp_folder, os.path.basename(station["parameters"][i]["file"]))
        df_n = fm.parse_csv(input_file, station["parameters"][i]["name"], "10min")
        os.remove(input_file)
        if i == 0:
            df = df_n
        else:
            df = df.merge(df_n, on="datetime")

    fm.write_to_meteolakes(df, station["name"], station["id"], station["columns"], out_folder)
fm.log("------------RUN COMPLETE------------", start=True)
server.close()
