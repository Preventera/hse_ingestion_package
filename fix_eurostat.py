from neo4j import GraphDatabase
from sqlalchemy import create_engine
import pandas as pd

pg = create_engine('postgresql://postgres:postgres@localhost:5432/safety_graph')
neo4j_driver = GraphDatabase.driver('bolt://localhost:7687')

df = pd.read_sql('''
    SELECT country_code, country_name, COUNT(*) as total
    FROM eurostat_esaw
    WHERE country_code IS NOT NULL
    GROUP BY country_code, country_name
''', pg)

print(f'Found {len(df)} EU countries')

with neo4j_driver.session() as session:
    for idx, row in df.iterrows():
        code = 'EU-' + str(row['country_code'])
        name = row['country_name'] or row['country_code']
        total = int(row['total'])
        try:
            session.run(
                "MERGE (c:Country {code: $code}) "
                "SET c.name = $name, c.total_records = $total, c.jurisdiction = 'EU27' "
                "WITH c "
                "MATCH (j:Jurisdiction {code: 'EU27'}) "
                "MERGE (c)-[:IN_JURISDICTION]->(j)",
                code=code, name=name, total=total
            )
            print(f'Added {code}')
        except Exception as e:
            print(f'Skip {code}: {e}')
    
    result = session.run('MATCH (c:Country) RETURN count(c) as count')
    print(f'Total Country nodes: {result.single()["count"]}')

neo4j_driver.close()
print('Done!')