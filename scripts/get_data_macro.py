import pandas as pd
from sqlalchemy import create_engine

def seed_macro_data(engine):
    # Données macro pour la région PACA (Provence-Alpes-Côte d'Azur)
    data = [
        {"region": "Provence-Alpes-Côte d'Azur", "annee": 2020, "taux_chomage": 9.0, "croissance_pib": -7.4, "indice_prix": 104.54},
        {"region": "Provence-Alpes-Côte d'Azur", "annee": 2021, "taux_chomage": 8.8, "croissance_pib": 6.9, "indice_prix": 105.12},
        {"region": "Provence-Alpes-Côte d'Azur", "annee": 2022, "taux_chomage": 8.1, "croissance_pib": 2.6, "indice_prix": 108.12},
        {"region": "Provence-Alpes-Côte d'Azur", "annee": 2023, "taux_chomage": 8.0, "croissance_pib": 0.9, "indice_prix": 114.60},
        {"region": "Provence-Alpes-Côte d'Azur", "annee": 2024, "taux_chomage": 7.8, "croissance_pib": None, "indice_prix": 118.19},
        {"region": "Provence-Alpes-Côte d'Azur", "annee": 2025, "taux_chomage": 8.0, "croissance_pib": None, "indice_prix": 120.14}
    ]
    
    df_macro = pd.DataFrame(data)
    
    try:
        df_macro.to_sql("macro_regional", con=engine, if_exists="replace", index=False)
        print("Table macro_regional mise à jour avec les indicateurs PACA.")
    except Exception as e:
        print("Erreur :", e)

if __name__ == "__main__":
    engine = create_engine("mysql+mysqlconnector://root:chiaramasi@localhost/entreprises_db")
    seed_macro_data(engine)
