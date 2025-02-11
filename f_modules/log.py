from datetime import timedelta, datetime
import logging
import pyodbc
import time

class DatabaseHandler(logging.Handler):
    def __init__(self, conn_str, customer, source, script, script_id):
        super().__init__()
        self.conn_str = conn_str
        self.customer = customer
        self.source = source
        self.script = script
        self.script_id = script_id
        try:
            # Maak verbinding met de Azure Database
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            # Zorg ervoor dat de table bestaat
            self.cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Logboek' AND xtype='U')
                CREATE TABLE Logboek (
                    ID INT IDENTITY PRIMARY KEY,
                    Niveau VARCHAR(50),
                    Bericht TEXT,
                    Datumtijd DATETIME,
                    Klant VARCHAR(100),
                    Bron VARCHAR(100),
                    Script VARCHAR(100),
                    Script_ID INT
                )
            ''')
            self.conn.commit()
        except Exception as e:
            # Fout bij het verbinden met de database of het maken van de tabel
            logging.error(f"Fout bij het verbinden met de database of het maken van de tabel: {e}")

    def emit(self, record):
        try:
            # Voeg de extra informatie toe aan het logbericht
            log_message = self.format(record)
            log_message = log_message.split('-')[-1].strip()
            
            # Converteer de tijd naar een string in het juiste formaat
            created_at = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
            
            # Voer de SQL-query uit om het logbericht in de database in te voegen
            self.cursor.execute("INSERT INTO Logboek (Niveau, Bericht, Datumtijd, Klant, Bron, Script, Script_ID) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (record.levelname, log_message, created_at, self.customer, self.source, self.script, self.script_id))
            self.conn.commit()
        except Exception as e:
            # Fout bij het invoegen van het logbericht in de database
            logging.error(f"Fout bij het invoegen van het logbericht in de database: {e}")

    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            logging.error(f"Fout bij het sluiten van de databaseverbinding: {e}")
        super().close()


# Functie om logging op te zetten
def setup_logging(conn_str, klant, bron, script, script_id, log_file='app.log', log_level=logging.INFO):
    # Configureer de basis logging
    logger = logging.getLogger()  # Haal de root logger op
    logger.setLevel(log_level)

    # Maak de handlers
    file_handler = logging.FileHandler(log_file)

    # Formatteer de logberichten voor de file handler
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)

    # Voeg de handlers toe aan de logger
    logger.addHandler(file_handler)

    # Voeg de DatabaseHandler toe aan de logger
    db_handler = DatabaseHandler(conn_str, klant, bron, script, script_id)
    db_handler.setFormatter(file_formatter)  # Gebruik dezelfde formatter voor de database
    logger.addHandler(db_handler)

    logging.info("Logboek is geconfigureerd.")

# Functie om de starttijd van het script te loggen
def start_log():
    start_time = time.time()
    current_time = datetime.now()
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Script started at {formatted_time}")
    
    return start_time

# Functie om de eindtijd van het script te loggen
def end_log(start_time):
    end_time = time.time()
    total_time = timedelta(seconds=(end_time - start_time))
    total_time_str = str(total_time).split('.')[0]
    logging.info(f"Script ended in {total_time_str}")
