import pandas as pd
import psycopg2

df = pd.read_excel("E:\LatLongByProDistSubDist.xlsx", sheet_name="tambon", usecols=["TA_ID","CH_ID","AM_ID","LAT","LONG","TAMBON_T","AMPHOE_T", "CHANGWAT_T"])
conn = psycopg2.connect(
    host="connection string here",
    port=5432,
    dbname="data name",
    user="user in posgress",
    password="your-password"
)
conn_4_4_3 = psycopg2.connect(
    host="connection string here",
    port=5432,
    dbname="data name",
    user="user in posgress",
    password="your-password"
)
cursor = conn.cursor()
cursor_4_4_3 = conn_4_4_3.cursor()
if cursor and cursor_4_4_3:
    print("Connect Success!")

code = pd.DataFrame(df)
i = 0
for idx, row in code.iterrows():
    i += 1
    AM_ID = row['AM_ID']
    TAM_B = row['TA_ID']
    CH_ID = row['CH_ID']
    TAMBON_T = row['TAMBON_T']
    AMPHOE_T = row['AMPHOE_T']
    CHANGWAT_T = row['CHANGWAT_T']
    LAT = row['LAT']
    LONG = row['LONG']
    AMPHOE_T_s = AMPHOE_T.split()
    TAMBON_T_s = TAMBON_T.split()
    CHANGWAT_T_s = CHANGWAT_T.split()
    print(f"=========Check Code{i}=============")
    print(f"แถว {idx} AM_ID: {AM_ID} TAM_B: {TAM_B} CH_ID: {CH_ID} LAT: {LAT} LONG: {LONG} {TAMBON_T_s} {AMPHOE_T_s} {CHANGWAT_T_s}")
    cursor.execute(
        "SELECT doh_sub_districts.id,doh_districts.id,doh_provinces.id,doh_sub_districts.name_th,doh_districts.name_th,doh_provinces.name_th FROM doh_districts JOIN doh_sub_districts ON doh_sub_districts.doh_district_id = doh_districts.id LEFT JOIN doh_provinces ON doh_districts.doh_province_id = doh_provinces.id WHERE doh_districts.name_th_short = %s AND doh_sub_districts.name_th_short = %s AND doh_provinces.name_th = %s" , (AMPHOE_T_s[1],TAMBON_T_s[1],CHANGWAT_T_s[1]))
    result = cursor.fetchone()
    if result:
        print("Update DataFrame")
        code.at[idx, "AMPHOE_T"] = result[0]  # อัพเดต DataFrame
        code.at[idx, "TAMBON_T"] = result[1]
        print(f"รหัสตำบล: {result[0]}  รหัสอำเภอ: {result[1]}  รหัสจังหวัด: {result[2]} ชื่อตำบล {result[3]} ชื่ออำเภอ: {result[4]} ชื่อจังหวัด:{result[5]}")
    else:
        print("ไม่พบข้อมูลในฐานแรก")
        continue
    cursor_4_4_3.execute(
        "UPDATE wc_establishments SET latitude = %s , longitude = %s WHERE doh_sub_district_id = %s AND latitude =0 AND longitude =0" ,(float(LAT),float(LONG),result[0])
    )
    print(f"UPDATE DATE Success! LAT: {LAT} LONG: {LONG}")
conn_4_4_3.commit()
cursor_4_4_3.close()
conn_4_4_3.close()