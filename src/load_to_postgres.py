import psycopg2
import pandas as pd

# Import des modules de transformation disponibles dans le dossier curated
from src.curated import customers, products, orders, orders_lines, payments, deliveries

# Connexion à PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres", # adapte le nom de ta base
    user="postgres",
    password="afri123" # adapte le mot de passe de ton utilisateur
)
cur = conn.cursor()
cur.execute("SET search_path TO curated;")

# === 1. Customers ===
df_customers = pd.read_csv("data/customers.csv")
df_cust_transformed, _, _ = customers.transform(df_customers)

for _, row in df_cust_transformed.iterrows():
    cur.execute("""
        INSERT INTO curated.dim_customer (
            customer_id, first_name, last_name, email_hash, phone_hash,
            country_code, city, gender, customer_segment,
            preferred_payment_method, loyalty_tier, account_status,
            date_debut_validite, is_current
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE)
        ON CONFLICT (customer_id) DO UPDATE
        SET first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            city = EXCLUDED.city,
            account_status = EXCLUDED.account_status;
    """, (
        row["customer_id"], row["first_name"], row["last_name"], row["email_hash"], row["phone_hash"],
        row["country_code"], row["city"], row["gender"], row["customer_segment"],
        row["preferred_payment_method"], row["loyalty_tier"], row["account_status"]
    ))
print("✅ Customers chargés")

# === 2. Products ===
df_products = pd.read_csv("data/products.csv")
df_prod_transformed, _, _ = products.transform(df_products)

for _, row in df_prod_transformed.iterrows():
    cur.execute("""
        INSERT INTO curated.dim_product (
            product_id, nom_produit, categorie, prix_unitaire,
            date_debut_validite, is_current
        )
        VALUES (%s, %s, %s, %s, CURRENT_DATE, TRUE)
        ON CONFLICT (product_id) DO UPDATE
        SET nom_produit = EXCLUDED.nom_produit,
            categorie = EXCLUDED.categorie,
            prix_unitaire = EXCLUDED.prix_unitaire;
    """, (
        row["product_id"], row["nom_produit"], row["categorie"], row["prix_unitaire"]
    ))
print("✅ Products chargés")

# === 3. Orders ===
df_orders = pd.read_csv("data/orders.csv")
df_orders_transformed, _, _ = orders.transform(df_orders)

for _, row in df_orders_transformed.iterrows():
    cur.execute("""
        INSERT INTO curated.fact_orders (
            order_id, customer_id, vendor_id, order_date, montant_total, status
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING;
    """, (
        row["order_id"], row["customer_id"], row["vendor_id"],
        row["order_date"], row["montant_total"], row["status"]
    ))
print("✅ Orders chargés")

# === 4. Order Lines ===
df_order_lines = pd.read_csv("data/order_lines.csv")
df_lines_transformed, _, _ = orders_lines.transform(df_order_lines)

for _, row in df_lines_transformed.iterrows():
    cur.execute("""
        INSERT INTO curated.fact_order_lines (
            order_line_id, order_id, product_id, quantite, prix_unitaire, remise
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_line_id) DO NOTHING;
    """, (
        row["order_line_id"], row["order_id"], row["product_id"],
        row["quantite"], row["prix_unitaire"], row["remise"]
    ))
print("✅ Order Lines chargés")

# === 5. Payments ===
df_payments = pd.read_csv("data/payments.csv")
df_pay_transformed, _, _ = payments.transform(df_payments)

for _, row in df_pay_transformed.iterrows():
    cur.execute("""
        INSERT INTO curated.fact_payments (
            payment_id, order_id, payment_date, montant, mode_paiement, status
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (payment_id) DO NOTHING;
    """, (
        row["payment_id"], row["order_id"], row["payment_date"],
        row["montant"], row["mode_paiement"], row["status"]
    ))
print("✅ Payments chargés")

# === 6. Deliveries ===
df_deliveries = pd.read_csv("data/deliveries.csv")
df_deliv_transformed, _, _ = deliveries.transform(df_deliveries)

for _, row in df_deliv_transformed.iterrows():
    cur.execute("""
        INSERT INTO curated.fact_deliveries (
            delivery_id, order_id, delivery_date, return_date, status_livraison
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (delivery_id) DO NOTHING;
    """, (
        row["delivery_id"], row["order_id"], row["delivery_date"],
        row["return_date"], row["status_livraison"]
    ))
print("✅ Deliveries chargés")

# Commit et fermeture
conn.commit()
cur.close()
conn.close()

print("🎉 Pipeline complet terminé avec succès !")
