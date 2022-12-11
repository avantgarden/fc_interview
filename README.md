FC interview

git clone https://github.com/avantgarden/fc_interview

pip install -r requirements.txt

python data_processor.py 

1. Load only usefull columns
df = pd.read_csv("Dataset-Unicauca-Version2-87Atts.csv", usecols=['Timestamp', 'Destination.IP', 'Flow.Bytes.s','Average.Packet.Size','Flow.Duration','ProtocolName'])

2. Load Timestamp as datetime object and remove unused time granularity (sec, min)
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d/%m/%Y%H:%M:%S').apply(lambda t: t.replace(minute=0, second=0))

3. Groupby and aggregation on dataframe + new index  
df = (df.groupby(["Timestamp","Destination.IP","ProtocolName"])
      .agg({
        "Flow.Duration": "sum",
        "Flow.Bytes.s": "mean",
        "Average.Packet.Size": "mean"      
    })).reset_index()

4. Count transfered bytes
def bytes_transfered(flow_duration, flow_bytes):
    bytes_transfered = flow_duration * flow_bytes
    return bytes_transfered

df['Bytes.Transfered'] = bytes_transfered(df['Flow.Duration'], df['Flow.Bytes.s'])

5. Count number of packets
def number_packets(bytes_transfered, packet_size):
    number_packets = bytes_transfered /  packet_size
    return number_packets

df['Number.packets'] = number_packets(df['Bytes.Transfered'], df['Average.Packet.Size'])

6. Fill 0 instead of NA/NaN
df['Number.packets'] = df['Number.packets'].fillna(0)

7. Convert Number.packets to integer
df['Number.packets'] = df['Number.packets'].astype(int)

8. Drop not longer usefull columns and shift Bytes.Transfered to the end.
df = df[["Timestamp","Destination.IP","ProtocolName", "Number.packets", "Bytes.Transfered"]]

10. Split dataframe by value in Timestamp
df_list = [d for _, d in df.groupby(['Timestamp'])]

11. Export each dataframe to csv with filename value from Timestamp
for i in df_list:
    filename = i['Timestamp'].iat[0]
    i.to_csv(f'export/{filename}.csv',sep=",")