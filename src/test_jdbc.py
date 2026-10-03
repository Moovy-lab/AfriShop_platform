from pyspark.sql import SparkSession

def main():
    # Création de la session Spark
    spark = SparkSession.builder \
        .appName("TestJDBCPostgres") \
        .getOrCreate()

    # Paramètres JDBC
    jdbc_url = "jdbc:postgresql://localhost:5432/afri_shop"  # adapte le nom de ta base
    jdbc_props = {
        "user": "postgres",          # ton utilisateur Postgres
        "password": "fayen",  # ton mot de passe
        "driver": "org.postgresql.Driver"
    }

    try:
        # Test : lecture d'une table système
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "information_schema.tables") \
            .options(**jdbc_props)\
            .load()

        print("✅ Connexion JDBC réussie, voici quelques tables :")
        df.show(5)

    except Exception as e:
        print("❌ Erreur de connexion JDBC :", e)

if __name__ == "__main__":
    main()
