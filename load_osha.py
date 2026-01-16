import pandas as pd
from sqlalchemy import create_engine

print('📥 Chargement des données OSHA...')
df = pd.read_csv('data/kaggle/ITA_OSHA_Combined.csv', low_memory=False)
print(f'✅ {len(df):,} lignes chargées')

print('🐘 Connexion à PostgreSQL...')
engine = create_engine('postgresql://postgres:postgres@localhost:5432/safety_graph')

print('💾 Insertion dans la base (peut prendre 1-2 minutes)...')
df.to_sql('osha_injuries_raw', engine, if_exists='replace', index=False, chunksize=10000)

print('✅ Données chargées dans PostgreSQL !')