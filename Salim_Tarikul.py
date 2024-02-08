import configparser
import requests 
import time
import os 
import pyfiglet
from tqdm import tqdm
import pyodbc
BASE_URL : str = 'https://geo.api.gouv.fr/communes'
HEADERS : str = {
        'content-type': "application/json",
        }

if __name__ == "__main__" :
    
    terminal_width, _ = os.get_terminal_size()
    text = f"Integrator"
    figlet = pyfiglet.Figlet(font='slant',width=terminal_width)
    print(figlet.renderText(text))
    
    config = configparser.ConfigParser()
    config_path = './TP_2/config.ini'

    config.read(config_path)
    database = config['sqlserver']['database']
    table = config['sqlserver']['table']
    server = config['sqlserver']['server']


    print("[*] Connect to the DB.")
    try : 
        conn = pyodbc.connect(
            f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        )
        cursor = conn.cursor() 
        print("[Success]\n")
    except Exception as e : 
        print(f"[Error] : {e}")
        exit()
    
    print(f"[*] Try to Create column named 'population' in {table}.")
    try : 
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN population INT;")
        conn.commit()
        print("[Success]\n")
    except Exception as e : 
        print("[*] Column population already exist ")
        
    
    cursor.execute(f"SELECT Code_commune FROM {table}")  
    resultats = cursor.fetchall()    
    
    count_rows = 0
    print(f"[*] Update {table}.")
    progress_bar = tqdm(total=len(resultats), desc="Get population", unit=" lines")
    
    for resultat in resultats: 
        if len (resultat[0]) <4 :
             continue
        para = f'code=0{resultat[0]}' if len(resultat[0]) == 4 else f'code={resultat[0]}'
        
        try : 
            response = requests.get(url=BASE_URL, params=para, headers=HEADERS)
            if response.status_code == 200 : 
                count_rows += 1
                data = response.json()
                if not len(data):
                     continue
                cursor.execute("UPDATE {} SET population = ? WHERE Code_commune = ?;".format(table), (int(data[0]['population']), resultat[0]))
                
        except requests.exceptions.HTTPError as errh:
                print(f"[Error]: {errh}")
        except requests.exceptions.ConnectionError as errc:
                print(f"[Error]: {errc}")
        except requests.exceptions.Timeout as errt:
                print(f"[Error]: {errt}")
        except requests.exceptions.RequestException as err:
                print(f"[Error]: {err}")
        
        progress_bar.update(1)
        
    conn.commit()
    conn.close()
    progress_bar.close()
    print(f"[Success] {count_rows}, rows updated\n")
    print("[Bye :)]\n")
    