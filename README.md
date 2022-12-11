## FC interview

```sh
git clone https://github.com/avantgarden/fc_interview
```
```sh
pip install -r requirements.txt
```
Download and unzip this dataset [https://www.kaggle.com/jsrojas/ip-network-traffic-flows-labeled-with-87-apps](https://www.kaggle.com/jsrojas/ip-network-traffic-flows-labeled-with-87-apps) 

```python
python data_processor.py 
```

1. Load only usefull columns
```python
df = pd.read_csv("Dataset-Unicauca-Version2-87Atts.csv", usecols=['Timestamp', 'Destination.IP', 'Flow.Bytes.s','Average.Packet.Size','Flow.Duration','ProtocolName'])
```

2. Load Timestamp as datetime object and remove unused time granularity (sec, min)
```python
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d/%m/%Y%H:%M:%S').apply(lambda t: t.replace(minute=0, second=0))
```

3. Groupby and aggregation on dataframe + new index
```python  
df = (df.groupby(["Timestamp","Destination.IP","ProtocolName"])
      .agg({
        "Flow.Duration": "sum",
        "Flow.Bytes.s": "mean",
        "Average.Packet.Size": "mean"      
    })).reset_index()
```

4. Count transfered bytes
```python
def bytes_transfered(flow_duration, flow_bytes):
    bytes_transfered = flow_duration * flow_bytes
    return bytes_transfered

df['Bytes.Transfered'] = bytes_transfered(df['Flow.Duration'], df['Flow.Bytes.s'])
```

5. Count number of packets
```python
def number_packets(bytes_transfered, packet_size):
    number_packets = bytes_transfered /  packet_size
    return number_packets

df['Number.packets'] = number_packets(df['Bytes.Transfered'], df['Average.Packet.Size'])
```

6. Fill 0 instead of NA/NaN
```python
df['Number.packets'] = df['Number.packets'].fillna(0)
```

7. Convert Number.packets to integer
```python
df['Number.packets'] = df['Number.packets'].astype(int)
```

8. Drop not longer usefull columns and shift Bytes.Transfered to the end.
```python
df = df[["Timestamp","Destination.IP","ProtocolName", "Number.packets", "Bytes.Transfered"]]
```

9. Split dataframe by value in Timestamp
```python
df_list = [d for _, d in df.groupby(['Timestamp'])]
```

10. Export each dataframe to csv with filename value from Timestamp
```python
for i in df_list:
    filename = i['Timestamp'].iat[0]
    i.to_csv(f'export/{filename}.csv',sep=",")
```