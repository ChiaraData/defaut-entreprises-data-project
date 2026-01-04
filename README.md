# defaut-entreprises-data-project
<h1 align="center">🛡️ Analyse et Prévision du Risque de Défaut des Entreprises (Aix-Marseille)</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/SQL-MySQL-orange.svg" alt="SQL">
  <img src="https://img.shields.io/badge/Machine_Learning-Random_Forest-green.svg" alt="ML">
</p>

##  Présentation du Projet
Ce projet vise à prédire la probabilité de défaillance des entreprises situées à **Aix-en-Provence** et **Marseille**. L'enjeu est de construire un **score de risque** fiable en utilisant des données publiques (APIs) pour pallier l'absence de données comptables privées.

## Architecture du Projet
Le projet repose sur une séparation stricte entre la collecte et l'analyse :

<ul>
  <li><b>/notebooks</b> :
    <ul>
      <li><code>data.ipynb</code> : Ingestion, tests API et logs d'investigation.</li>
      <li><code>modelisation_risque.ipynb</code> : EDA, preprocessing et Machine Learning.</li>
    </ul>
  </li>
  <li><b>/scripts</b> : 
    <ul>
      <li><code>get_data_api.py</code> : Collecte SIRENE (Établissements).</li>
      <li><code>data_annonces.py</code> : Extraction BODACC (Procédures collectives).</li>
      <li><code>get_data_macro.py</code> : donnée macro régionale.</li>
      <li><code>schema.sql</code> : Structure de la base de données MySQL.</li>
      <li><code>data_final.sql</code> : Structure de la table final MySQL.</li>
    </ul>
  </li>
  <li><b>/data</b> :
  <ul>
      <li><code>dataset_risque_entreprises.csv</code> : données final</li>
  </ul>
  </li>
  <li><b>/utils</b> :
  <ul>
      <li><code>config.py</code> : configuration. </li>
    </ul>
  </li>
</ul>



## Défis Rencontrés & Adaptations
En tant qu'étudiante, j'ai dû faire face à des contraintes réelles de terrain :
<ul>
  <li><b>Accessibilité des données</b> : Les APIs financières (Pappers, INPI) étant payantes ou restreintes, la table <code>comptes_annuels</code> n'a pas pu être alimentée.</li>
  <li><b>Pivot Stratégique</b> : J'ai utilisé des <b>signaux "Proxy"</b> : les avis de liquidation/redressement du BODACC comme variable cible, combinés aux caractéristiques structurelles (NAF, âge, effectifs).</li>
  <li><b>Contexte Macro</b> : Intégration des taux de chômage et PIB régionaux (PACA) pour enrichir le modèle.</li>
</ul>

## Résultats et Machine Learning
<ul>
  <li><b>Modèle</b> : Random Forest Classifier.</li>
  <li><b>Gestion du déséquilibre</b> : Utilisation de <code>class_weight='balanced'</code> pour traiter un dataset comportant moins de 1% de défauts.</li>
  <li><b>Features clés</b> : L'ancienneté de l'entreprise et le secteur d'activité (code NAF) sont les principaux leviers de prédiction identifiés.</li>
</ul>

---
<p align="center"><i>Projet réalisé pour démontrer des compétences en ETL, SQL et modélisation prédictive.</i></p>