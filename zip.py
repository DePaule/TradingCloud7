import os
import pyperclip

def is_valid_file(filename):
    """
    Erlaubte Dateien: .yml, .json, .ts, .tsx, .css, .py oder Dateien, deren Name (ohne Extension) "dockerfile" (case-insensitive) ist.
    Dateien namens "zip.py" werden niemals berücksichtigt.
    """
    if filename.lower() == "zip.py":
        return False  # Muss vor dem Extension-Check erfolgen!
    allowed_extensions = {'.yml', '.json', '.ts', '.tsx', '.css', '.py'}
    base, ext = os.path.splitext(filename)
    if ext.lower() in allowed_extensions:
        return True
    if filename.lower() == "dockerfile":
        return True
    return False

def read_file_contents(filepath):
    """
    Liest den Inhalt einer Datei als Text (UTF-8). Bei Problemen wird versucht, ohne explizite Encoding-Angabe zu lesen.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        try:
            with open(filepath, 'r') as f:
                return f.read()
        except Exception as e:
            print(f"Fehler beim Lesen von {filepath}: {e}")
            return None

def collect_files(root_dir):
    """
    Durchsucht nur die folgenden Ordner relativ zum root_dir:
      - root (.)
      - /frontend
      - /frontend/src
      - /frontend/src/components
      - /backend
      - /backend/datasources
    Sammelt alle gültigen Dateien (direkt in diesen Ordnern, keine Rekursion in weitere Unterordner),
    und gibt eine Liste von Tupeln (relativer Pfad, Inhalt) zurück.
    """
    collected = []
    # Definierte Verzeichnisse relativ zum root_dir
    search_dirs = [
        ".",
        "frontend",
        os.path.join("frontend", "src"),
        os.path.join("frontend", "src", "components"),
        "backend",
        os.path.join("backend", "app", "datasources"),
        os.path.join("backend", "app")
    ]
    for rel_dir in search_dirs:
        abs_dir = os.path.join(root_dir, rel_dir)
        if not os.path.isdir(abs_dir):
            continue
        for filename in os.listdir(abs_dir):
            full_path = os.path.join(abs_dir, filename)
            if not os.path.isfile(full_path):
                continue
            if not is_valid_file(filename):
                continue
            # Relativer Pfad vom root_dir, mit führendem Slash und "/" als Trenner
            rel_path = os.path.relpath(full_path, root_dir)
            rel_path_formatted = "/" + rel_path.replace(os.sep, "/")
            content = read_file_contents(full_path)
            if content is not None:
                collected.append((rel_path_formatted, content))
    return collected

def main():
    root_dir = os.getcwd()  # Starte im Root-Ordner der Solution
    files = collect_files(root_dir)
    # Erzeuge einen String, in dem jede Datei als Zeile steht:
    # <relativer Pfad> "Dateiinhalt"
    entries = []
    for rel_path, content in files:
        # Doppelte Anführungszeichen im Inhalt escapen
        escaped_content = content.replace('"', '\\"')
        entry = f'{rel_path} "{escaped_content}"'
        entries.append(entry)
    result = "\n".join(entries)
    pyperclip.copy(result)
    print("Alle gültigen Dateien wurden gesammelt und in den Zwischenspeicher geladen.")

if __name__ == "__main__":
    main()
