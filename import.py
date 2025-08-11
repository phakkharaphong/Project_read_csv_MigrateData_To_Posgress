import psycopg2

conn = psycopg2.connect(
    host="connection string",
    port=5432,
    dbname="ehdc_4_4_3",
    user="dbmasteruser",
    password="password"
)
cursor = conn.cursor()

#with open('E:/import_wc_establishments.csv', 'r', encoding='utf-8') as f:
#    cursor.copy_expert("""
#        COPY public.wc_establishments (
#            id, wc_category_id, name, created_at,updated_at, doh_sub_district_id,
#            men_toilet_count, men_urinal_count, men_handwash_sink_count,
#            women_toilet_count, women_handwash_sink_count, disabled_toilet_count,latitude,longitude
#        )
#        FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"' ESCAPE ''''
#    """, f)

#with open('E:\import_wc_assessment_forms.csv', 'r', encoding='utf-8') as f:
#    cursor.copy_expert("""
#        COPY public.wc_establishment_assessment (
#            id, wc_establishment_id, self_approval_id, self_approval_status,self_approval_date, officer_approval_id,
#            officer_approval_status, officer_approval_date, assessment_expires_at,
#            created_at, updated_at, wc_assessment_form_id,fiscal_year
#        )
#        FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"' ESCAPE ''''
#    """, f)

with open('E:\import_wc_assessments.csv', 'r', encoding='utf-8') as f:
    cursor.copy_expert("""
        COPY public.wc_assessments (
            id, wc_standard_criteria_id,is_pass,created_at,updated_at,wc_establishment_assessment_id,image_public_id,image_public_url,image_public_name
        )
        FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"' ESCAPE ''''
    """, f)

conn.commit()
cursor.close()
conn.close()