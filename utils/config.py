import os
from dotenv import load_dotenv

base_url = "https://api.insee.fr/entreprises/sirene/V3/siret"

load_dotenv()  # charge les variables d'environnement depuis .env

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
SIRENE_API_KEY = os.getenv("SIRENE_API_KEY")
