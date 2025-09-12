import requests, time, pandas as pd
from requests.exceptions import RequestException

API_URL = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records"

def get_existing_sirens(engine):
    try:
        return set(pd.read_sql("SELECT siren FROM sirene", engine)["siren"].astype(str))
    except Exception as e:
        print("Erreur lecture SIREN :", e)
        return set()

def fetch_bodacc(engine, date_debut="2020-01-01", use_siren_filter=True):
    sirens = get_existing_sirens(engine) if use_siren_filter else set()
    if use_siren_filter and not sirens:
        print("Aucun SIREN en base → rien à insérer")
        return pd.DataFrame()

    params = {"limit": 100, "refine": 'departement_nom_officiel:"Bouches-du-Rhône"',
              "where": f"dateparution>='{date_debut}'"}
    offset, all_results = 0, []
    while offset <= 9900:
        params["offset"] = offset
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            if r.status_code != 200:
                print("Erreur API :", r.status_code, r.text)
                break

            data = r.json()
            recs = data.get("results", [])
            if not recs:
                break

            for rec in recs:
                siren = rec.get("registre")
                if isinstance(siren, list) and siren:
                    s = siren[0].replace(" ", "")
                elif isinstance(siren, str):
                    s = siren.replace(" ", "")
                else:
                    continue

                if use_siren_filter and s not in sirens:
                    continue
                rec["siren"] = s
                all_results.append(rec)

            offset += len(recs)
            if len(recs) < params["limit"]:
                break
            time.sleep(0.5)
        except RequestException as e:
            print("Erreur réseau :", e)
            break

    if not all_results:
        print("Aucun enregistrement trouvé")
        return pd.DataFrame()

    df = pd.json_normalize(all_results, sep="_")
    cols = {"siren": "siren", "familleavis_lib": "type_procedure",
            "dateparution": "date_procedure", "url_complete": "source"}
    df_sql = df[[c for c in cols if c in df.columns]].rename(columns=cols)
    df_sql["date_procedure"] = pd.to_datetime(df_sql["date_procedure"], errors="coerce")
    df_sql = df_sql.dropna(subset=["siren"]).drop_duplicates(["siren", "date_procedure"])

    try:
        df_sql.to_sql("bodacc_procedures", con=engine, if_exists="append",
                      index=False, method="multi")
        print(f"{len(df_sql)} lignes insérées dans bodacc_procedures")
    except Exception as e:
        print("Erreur insertion :", e)
    return df_sql
