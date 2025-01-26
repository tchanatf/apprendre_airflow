import duckdb

# Définir le chemin du fichier DuckDB
db_path = "d:\\cours_udemy_airflow\\dags\\data\\my_bdd_airflow.duckdb"

# Créer une connexion à la base de données (le fichier sera créé s'il n'existe pas)
con = duckdb.connect(db_path)

# Créer une table et insérer des données d'exemple
con.execute("""
    CREATE TABLE data_from_prim (
        name STRING,
        update_at STRING,
        objectid STRING
    )
""")


# Vérifier que les données sont bien insérées
result = con.execute("SELECT * FROM data_from_prim").fetchall()
print("Données insérées :", result)

# Fermer la connexion
con.close()
