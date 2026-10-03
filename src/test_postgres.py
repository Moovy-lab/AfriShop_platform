import psycopg2

# Connexion à Postgres (conteneur Docker)
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="afri_shop",   # ou afri_shop si tu as créé une DB spécifique
    user="postgres",
    password="fayen"
)

cur = conn.cursor()

# Utiliser le schéma curated
cur.execute("SET search_path TO curated;")

# Insertion de test
cur.execute("""
    INSERT INTO dim_customer (
        customer_id, first_name, last_name, email_hash, country_code, city,
        gender, customer_segment, preferred_payment_method, loyalty_tier,
        account_status, date_debut_validite, is_current
    )
    VALUES (%s, %s, %s, md5(%s), %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE)
""", ("TEST003", "Faye", "Djiby", "djiby@example.com", "SN", "Thiès", "M", "VIP", "Carte", "Gold", "Active"))

conn.commit()

# Vérification
cur.execute("SELECT * FROM dim_customer LIMIT 5;")
rows = cur.fetchall()
for row in rows:
    print(row)

cur.close()
conn.close()
