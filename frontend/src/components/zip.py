import os
import pyperclip

def is_valid_file(filename):
    """
    Erlaubte Dateien: .py, .json, .html, .ts, .css oder Dateien, deren Name (ohne Extension) "dockerfile" (case-insensitive) ist.
    """
    if filename.lower() == "zip.py":
       return False  # Muss VOR dem Extension-Check kommen!
    allowed_extensions = {'.py', '.json', '.html', '.ts', '.css', '.jsx'}
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
    Durchläuft rekursiv das root_dir und sammelt alle gültigen Dateien,
    sofern sie nicht im 'venv'-Ordner liegen und höchstens 3 Ordner-Ebenen tief sind.
    Gibt eine Liste von Tupeln (relativer Pfad, Inhalt) zurück.
    """
    collected = []
    for dirpath, _, filenames in os.walk(root_dir):
        # Normiere den Pfad und überspringe Verzeichnisse, die 'venv' enthalten
        norm_dir = os.path.normpath(dirpath)
        if 'venv' in norm_dir.split(os.sep):
            continue

        for filename in filenames:
            if not is_valid_file(filename):
                continue

            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, root_dir)
            parts = rel_path.split(os.sep)
            # Die Tiefe beträgt die Anzahl der Ordner, also len(parts)-1; max. 3 Ebenen erlaubt => len(parts) max. 4
            if len(parts) > 4:
                continue

            # Formatierung des relativen Pfads: führender Slash, immer "/" als Trenner
            rel_path_formatted = "/" + rel_path.replace(os.sep, '/')
            content = read_file_contents(full_path)
            if content is not None:
                collected.append((rel_path_formatted, content))
    return collected

def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    files = collect_files(root_dir)
    # Erzeuge einen String, in dem jede Datei als Zeile steht:
    # <relativer Pfad> "Dateiinhalt"
    entries = []
    for rel_path, content in files:
        # Falls der Inhalt doppelte Anführungszeichen enthält, kannst du diese auch escapen
        escaped_content = content.replace('"', '\\"')
        entry = f'{rel_path} "{escaped_content}"'
        entries.append(entry)
    result = "\n".join(entries)
    pyperclip.copy(result)
    print("Alle gültigen Dateien (ohne venv und mit maximal 3 Ordner-Ebenen) wurden gesammelt und in den Zwischenspeicher geladen.")

if __name__ == "__main__":
    main()
