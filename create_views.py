from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine('postgresql://postgres:postgres@localhost:5432/safety_graph')

print('Creating views...')

with engine.connect() as conn:
    conn.execute(text('CREATE OR REPLACE VIEW safetwin_top_sectors AS SELECT "SECTEUR_SCIAN" as secteur, COUNT(*) as total FROM cnesst_lesions_quebec GROUP BY "SECTEUR_SCIAN" ORDER BY total DESC'))
    conn.commit()
    print('View 1 created')
    
    conn.execute(text('CREATE OR REPLACE VIEW safetwin_tendances AS SELECT "ANNEE" as annee, COUNT(*) as total FROM cnesst_lesions_quebec GROUP BY "ANNEE" ORDER BY "ANNEE"'))
    conn.commit()
    print('View 2 created')

print('')
print('Top 5 Secteurs:')
print(pd.read_sql('SELECT * FROM safetwin_top_sectors LIMIT 5', engine))
print('')
print('Tendances:')
print(pd.read_sql('SELECT * FROM safetwin_tendances', engine))
