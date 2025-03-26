import psycopg2
import sys

# Passe diesen DSN an deine tatsächlichen Verbindungsdaten an:
# - Host (db oder localhost)
# - Port (5432)
# - Benutzername (trader)
# - Passwort (trader)
# - Datenbankname (tradingcloud)
DSN = "postgresql://trader:trader@db:5432/tradingcloud"

def main():
    print(f"Verbindungs-DSN: {DSN}")
    try:
        # Verbindung zur Datenbank herstellen
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                # Einfache Abfrage, um die Version anzuzeigen
                cur.execute("SELECT version()")
                version = cur.fetchone()
                print("DB-Version:", version[0])
    except Exception as e:
        print("Fehler beim Verbinden mit der Datenbank:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
