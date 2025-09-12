import requests
import pandas as pd
import time
from utils.config import base_url, SIRENE_API_KEY 

# --------------------
# Paramètres API SIRENE
# --------------------
API_URL = base_url
API_KEY = SIRENE_API_KEY

HEADERS = {
    "X-INSEE-Api-Key-Integration": API_KEY
}
# --------------------
# Fonction pour récupérer les entreprises d'une ville
# --------------------
def get_entreprises_by_city(ville: str, engine=None):
    """
    Récupère toutes les entreprises pour une ville donnée et les retourne sous forme de DataFrame.
    Insère directement les données dans MySQL.
    """
    print(f"Récupération des entreprises pour la ville : {ville}")
    
    all_results = []
    params = {
        "q": f"libelleCommuneEtablissement:{ville}",
        "nombre": 10000  # maximum par page
    }
    
    cursor = None
    while True:
        if cursor:
            params["curseur"] = cursor
        
        response = requests.get(API_URL, headers=HEADERS, params=params)
        
        if response.status_code != 200:
            print("Erreur API :", response.status_code, response.text)
            break
        
        data = response.json()
        etablissements = data.get("etablissements", [])
        all_results.extend(etablissements)
        
        print(f"Page récupérée, total cumulé = {len(all_results)}")
        
        # Récupération du curseur suivant
        cursor = data.get("header", {}).get("curseurSuivant")
        if not cursor:
            break
        
        # Petite pause pour éviter les blocages API
        time.sleep(0.5)
    
    # --------------------
    # Conversion en DataFrame
    # --------------------
    df = pd.json_normalize(all_results, sep='.')
    
    # --------------------
    # Sélection des colonnes
    # --------------------
    colonnes_a_garder = {
        'siret': 'siret',
        'siren': 'siren',
        'uniteLegale.denominationUniteLegale': 'denomination',
        'uniteLegale.activitePrincipaleUniteLegale': 'naf_code',
        'uniteLegale.dateCreationUniteLegale': 'date_creation',
        'uniteLegale.trancheEffectifsUniteLegale': 'effectif',
        'adresseEtablissement.libelleVoieEtablissement': 'adresse',
        'adresseEtablissement.codePostalEtablissement': 'code_postal',
        'adresseEtablissement.libelleCommuneEtablissement': 'ville'
    }
    
    colonnes_existantes = [c for c in colonnes_a_garder.keys() if c in df.columns]
    df_sql = df[colonnes_existantes].rename(columns={c: colonnes_a_garder[c] for c in colonnes_existantes})
    
    print("Colonnes après rename :", df_sql.columns)
    print(df_sql.head())

    # --------------------
    # Nettoyage date et effectif
    # --------------------
    df_sql["date_creation"] = pd.to_datetime(df_sql["date_creation"], errors="coerce")
    
    effectif_mapping = {
        "00": "0 salarié", "01": "1-2", "02": "3-5", "03": "6-9",
        "11": "10-19", "12": "20-49", "21": "50-99", "22": "100-199",
        "31": "200-249", "32": "250-499", "41": "500-999", "42": "1000-1999",
        "51": "2000-4999", "52": "5000-9999", "53": "10000+", "NN": None
    }
    df_sql["effectif"] = df_sql["effectif"].map(effectif_mapping)
    
    # --------------------
    # Supprimer les doublons
    # --------------------
    df_sql = df_sql.drop_duplicates(subset=["siret"])
    
    # --------------------
    # Optionnel : insertion MySQL
    # --------------------
    if engine:
        df_sql.to_sql("sirene", con=engine, if_exists="append", index=False)
        print(f"Données pour {ville} insérées en base avec succès !")

    return df_sql

def get_entreprises_by_codes(codes_postaux: list, engine=None):
    
    all_results = []

    # --------------------
    # Boucle sur les codes postaux
    # --------------------
    for cp in codes_postaux:
        print(f"Récupération des entreprises pour CP {cp}...")
        params = {
            "q": f"codePostalEtablissement:{cp}",
            "nombre": 1000
        }

        cursor = None
        while True:
            if cursor:
                params["curseur"] = cursor

            response = requests.get(API_URL, headers=HEADERS, params=params)

            if response.status_code != 200:
                print("Erreur API :", response.status_code, response.text)
                break

            data = response.json()
            etablissements = data.get("etablissements", [])
            all_results.extend(etablissements)

            print(f"Page récupérée pour {cp}, total cumulé = {len(all_results)}")

            # Récupération du curseur suivant
            cursor = data.get("header", {}).get("curseurSuivant")
            if not cursor:
                break

    # --------------------
    # Conversion en DataFrame
    # --------------------
    df = pd.json_normalize(all_results, sep='.')

    # --------------------
    # Sélection des colonnes
    # --------------------
    colonnes_a_garder = {
        'siret': 'siret',
        'siren': 'siren',
        'uniteLegale.denominationUniteLegale': 'denomination',
        'uniteLegale.activitePrincipaleUniteLegale': 'naf_code',
        'uniteLegale.dateCreationUniteLegale': 'date_creation',
        'uniteLegale.trancheEffectifsUniteLegale': 'effectif',
        'adresseEtablissement.libelleVoieEtablissement': 'adresse',
        'adresseEtablissement.codePostalEtablissement': 'code_postal',
        'adresseEtablissement.libelleCommuneEtablissement': 'ville'
    }

    colonnes_existantes = [c for c in colonnes_a_garder.keys() if c in df.columns]
    df_sql = df[colonnes_existantes].rename(columns={c: colonnes_a_garder[c] for c in colonnes_existantes})

    # --------------------
    # Nettoyage date
    # --------------------
    df_sql["date_creation"] = pd.to_datetime(df_sql["date_creation"], errors="coerce")

    # --------------------
    # Nettoyage de la colonne effectif
    # --------------------
    effectif_mapping = {
        "00": "0 salarié",
        "01": "1-2",
        "02": "3-5",
        "03": "6-9",
        "11": "10-19",
        "12": "20-49",
        "21": "50-99",
        "22": "100-199",
        "31": "200-249",
        "32": "250-499",
        "41": "500-999",
        "42": "1000-1999",
        "51": "2000-4999",
        "52": "5000-9999",
        "53": "10000+",
        "NN": None
    }

    df_sql["effectif"] = df_sql["effectif"].map(effectif_mapping)

    # --------------------
    # Supprimer les doublons sur le SIRET
    # --------------------
    df_sql = df_sql.drop_duplicates(subset=["siret"])

    if engine:
        df_sql.to_sql("sirene", con=engine, if_exists="append", index=False)
        print(f"Données pour {codes_postaux} insérées en base avec succès !")

    return df_sql