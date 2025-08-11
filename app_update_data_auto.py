import pandas as pd
import psycopg2

#df = pd.read_excel("E:\export_has110668.xlsx", sheet_name="em_from", usecols=["id","ว/ด/ป ลงทะเบียน","ว/ด/ป สิ้นสุด"])
df = pd.read_excel("E:\export_has110668.xlsx", sheet_name="em_from", usecols=["id","ว/ด/ป ลงทะเบียน"])
conn = psycopg2.connect(
    host="connection string",
    port=5432,
    dbname="ehdc_4_4_3",
    user="dbmasteruser",
    password="password"
)
cursor = conn.cursor()
print("Connect Success!")

i = 0
#for idx, row in df.iterrows():
#    if pd.notnull(row["id"]) and pd.notnull(row["ว/ด/ป ลงทะเบียน"]) and pd.notnull(row["ว/ด/ป สิ้นสุด"]):
#        i += 1
#        regis_date = row["ว/ด/ป ลงทะเบียน"]
#        end_date = row["ว/ด/ป สิ้นสุด"]
#        id_es = row["id"]
#        print(f"=================== Index [{i}] ========================")
#        print(f"regis_date: {regis_date} and end_date: {end_date}")
#        cursor.execute(
#            "UPDATE public.wc_establishment_assessment SET self_approval_date = %s, assessment_expires_at = %s WHERE wc_establishment_id = %s",
#            (regis_date, end_date, id_es)
#        )
#        print(f"UPDATE DATE Success! ID: {id_es}")

for idx, row in df.iterrows():
   if pd.notnull(row["id"]) and pd.notnull(row["ว/ด/ป ลงทะเบียน"]):
        i += 1
        regis_date = row["ว/ด/ป ลงทะเบียน"]
        id_es = row["id"]
        print(f"=================== Index [{i}] ========================")
        print(f"officer_approval_date: {regis_date} ")
        cursor.execute(
            "UPDATE public.wc_establishment_assessment SET officer_approval_date = %s WHERE wc_establishment_id = %s",
            (regis_date, id_es)
        )
        print(f"UPDATE DATE Success! ID: {id_es} officer_approval_date: {cursor.fetchone}")

conn.commit()  # บันทึกการเปลี่ยนแปลง
cursor.close()
conn.close()
