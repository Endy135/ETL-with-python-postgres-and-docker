import subprocess
import time



def wait_for_postgres(host, max_essai=5, delai_secondes=5):
    essai = 0
    while essai < max_essai:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                print("Connecté à postgres avec succès!!!!")
                return True
            
        except subprocess.CalledProcessError as e:
            print(f"Erreur de connections à postgres{e}")
            essai += 1
            print(
                f"Nouvelle tentative dans un delai de {delai_secondes} seconds... (tentative {essai} / {max_essai}")
            time.sleep(delai_secondes)
    print("Nombre maximum d'essai atteint. Terminé")
    return False

if not wait_for_postgres(host = "source_postgres"):
    exit(1)

print('Demarrage du script d\'ELT')

source_config = {
        'dbname' : 'source_db',
        'user': 'postgres',
        'password': 'postgres',
        'host' : 'source_postgres'
    }

destination_config = {
        'dbname' : 'destination_db',
        'user': 'postgres',
        'password': 'postgres',
        'host' : 'destination_postgres'
    }

commande_sauvegarde = [
    'pg_dump', 
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD = source_config['password'])

subprocess.run(commande_sauvegarde, env = subprocess_env, check=True)

commande_chargement = [
    'psql', 
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

subprocess_env = dict(PGPASSWORD = destination_config['password'])

subprocess.run(commande_chargement, env=subprocess_env, check=True)


print("Processus d'ELT terminé")