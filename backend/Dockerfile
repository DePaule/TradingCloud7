FROM python:3.9

WORKDIR /app

# Kopiere nur zunächst die Requirements-Datei und installiere die Abhängigkeiten
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den restlichen Code (inklusive app/main.py, importer.py, etc.)
COPY . .

# Exponiere sowohl den App-Port als auch den Debug-Port
EXPOSE 8000
EXPOSE 5678

# Starte Uvicorn mit dem korrekten Modulpfad und --reload (beachte: --reload kann Debugger-Sitzungen beeinflussen)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]