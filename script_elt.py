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
                f"Nouvelle tentative dans une delai de {delai_secondes} seconds... (Attemp {essai / {max_essai}}")
            time.sleep(delai_secondes)
    print("Nombre maximum d'essai atteind. Terminé")
    return False

if not wait_for_postgres(host = "source_postgres"):
    exit(1)

print('Demarrage du script d\'ELT')

source_config = {
        'dbname' : 'source_db',
        'user': 'postgres',
        'password': 'postgres',
        'host' : 'destination_db'
    }

destination_config = {
        'dbname' : 'destination_db',
        'user': 'postgres',
        'password': 'postgres',
        'host' : 'destination_postgres'
    }

dump_command = [
    'pg_dump', 
    '-h', source_config['host'],
    '-u', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD = source_config['password'])

subprocess.run(dump_command, env = subprocess_envn check=True)

load_command = [
    
]