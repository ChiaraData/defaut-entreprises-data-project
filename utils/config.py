import os

# Configuration de la base de données
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'chiaramasi',
    'database': 'entreprises_db',
    'charset': 'utf8'
}

# clé API pour accéder aux données 
base_url = "https://api.insee.fr/entreprises/sirene/V3/siret"
clé_API_SIRENE = "2c6da094-b893-415a-ada0-94b893115aea"