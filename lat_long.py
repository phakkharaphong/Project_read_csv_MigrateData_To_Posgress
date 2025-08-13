import psycopg2
from geopy.geocoders import Nominatim
import time
import pandas as pd

conn = psycopg2.connect(
    host="connection string here",
    port=5432,
    dbname="data name",
    user="user in posgress",
    password="your-password"
)
cursor = conn.cursor()


geolocator = Nominatim(user_agent="my_geocoder_app")
def get_lat_lon(tambon_name, province=None, country="Thailand"):
    query = tambon_name
    if province:
        query += ", " + province
    query += ", " + country
    
    location = geolocator.geocode(query, timeout=10)
    if location:
        return location.latitude, location.longitude
    else:
        return None, None

# ตัวอย่างใช้
#tambon = "บางรัก"
#province = "กรุงเทพมหานคร"
#lat, lon = get_lat_lon(tambon, province)
#print(f"ตำบล {tambon} ละติจูด: {lat}, ลองจิจูด: {lon}")


df = pd.read_excel("E:\export_has110668.xlsx", sheet_name="em_from", usecols=[ "จังหวัด","ตำบล","latitude","longitude"])
code = pd.DataFrame(df)
for idx, row in code.iterrows():
    if pd.isnull(row["latitude"]) and pd.isnull(row["longitude"]):
        tambon = row['ตำบล']
        province = row['จังหวัด']
        print(f"แถว {idx} ค่าคอลัมน์ latitude and longitude เป็น null")
        lat, lon = get_lat_lon(tambon, province)
        print(f"ตำบล {tambon} จังหวัด {province} ละติจูด: {lat}, ลองจิจูด: {lon}")
        if lat is None and lon is None:
            code.at[idx, "latitude"] = lat
            code.at[idx, "longitude"] = lon
            print(f"lat: {lat} long: {lon}")
        else:
            print(f"แถว {idx} tambon = {row['ตำบล']} province = {row['จังหวัด']}")
code.to_excel(r"E:\export_has110669.xlsx", sheet_name="latlong", index=False)

cursor.close()
conn.close()
print("บันทึกข้อมูลลง Excel เรียบร้อย")