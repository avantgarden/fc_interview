import os
import pandas as pd

if not os.path.exists('export'):
   os.makedirs('export')

# load only usefull columns
print('Loading data...\n')
df = pd.read_csv("Dataset-Unicauca-Version2-87Atts.csv", usecols=['Timestamp', 'Destination.IP', 'Flow.Bytes.s','Average.Packet.Size','Flow.Duration','ProtocolName'])

# Load Timestamp as datetime object and remove unused time granularity (sec, min)
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d/%m/%Y%H:%M:%S').apply(lambda t: t.replace(minute=0, second=0))

print(df)
print('Groupby processing...\n')
# Groupby and aggregation on dataframe + new index  
df = (df.groupby(["Timestamp","Destination.IP","ProtocolName"])
      .agg({
        "Flow.Duration": "sum",
        "Flow.Bytes.s": "mean",
        "Average.Packet.Size": "mean"      
    })).reset_index()


# Count transfered bytes
def bytes_transfered(flow_duration, flow_bytes):
    bytes_transfered = flow_duration * flow_bytes
    return bytes_transfered

df['Bytes.Transfered'] = bytes_transfered(df['Flow.Duration'], df['Flow.Bytes.s'])

# Count number of packets
def number_packets(bytes_transfered, packet_size):
    number_packets = bytes_transfered /  packet_size
    return number_packets

df['Number.packets'] = number_packets(df['Bytes.Transfered'], df['Average.Packet.Size'])


# Fill 0 instead of NA/NaN
df['Number.packets'] = df['Number.packets'].fillna(0)

# Number of packets should be integer
df['Number.packets'] = df['Number.packets'].astype(int)

# Drop not longer usefull columns and shift Bytes.Transfered to the end.
df = df[["Timestamp","Destination.IP","ProtocolName", "Number.packets", "Bytes.Transfered"]]

# Split dataframe by value in Timestamp
df_list = [d for _, d in df.groupby(['Timestamp'])]
print('\n')
print('Exporting data to csv \n')
# Export each dataframe to csv with filename value from Timestamp
for i in df_list:
    filename = i['Timestamp'].iat[0]
    i.to_csv(f'export/{filename}.csv',sep=",")
    print(f'export/{filename}.csv')

print('Export done, results are in export dir.')

