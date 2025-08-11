import pandas as pd
import psycopg2

# อ่านไฟล์ Excel
df = pd.read_excel("E:\export_has110668.xlsx", sheet_name="em_from", usecols=["จังหวัด","อำเภอ", "code", "ตำบล"])

# ตั้งค่าการเชื่อมต่อ PostgreSQL
conn = psycopg2.connect(
    host="connection string",
    port=5432,
    dbname="ehdc_4_2",
    user="dbmasteruser",
    password="password"
)
cursor = conn.cursor()
if cursor:
    print("Connect Success!")

code = pd.DataFrame(df)
i = 0

for idx, row in code.iterrows():
    if pd.isnull(row["code"]):
        i += 1
        tambon = row['ตำบล']
        province = row['จังหวัด']
        amphur = row['อำเภอ']
        print(f"=========Check Code{i}=============")
        print(f"แถว {idx} ค่าคอลัมน์ code เป็น null จังหวัด:{province} อำเภอ:{amphur} ตำบล: {tambon}")
        cursor.execute(
            "SELECT doh_sub_districts.id,doh_districts.id,doh_sub_districts.name_th,doh_districts.name_th FROM doh_districts JOIN doh_sub_districts ON doh_sub_districts.doh_district_id = doh_districts.id LEFT JOIN doh_provinces ON doh_districts.doh_province_id = doh_provinces.id WHERE doh_districts.name_th_short ILIKE %s AND doh_sub_districts.name_th ILIKE %s AND doh_provinces.name_th ILIKE %s" , (amphur,tambon,province))
        result = cursor.fetchone()
        if result:
            print("Update DataFrame")
            code.at[idx, "code"] = result[0]  # อัพเดต DataFrame
            print(f"code: {result[0]} districts_id: {result[1]} ตำบลคือ: {result[2]}")
    else:
        i += 1
        print(f"แถว {idx} code = {row['code']}")

# บันทึกข้อมูลกลับลง Excel หลังจบลูป
code.to_excel(r"E:\export_has110669.xlsx", sheet_name="em_from", index=False)

cursor.close()
conn.close()
print("บันทึกข้อมูลลง Excel เรียบร้อย")

