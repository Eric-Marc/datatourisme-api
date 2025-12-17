#!/usr/bin/env python3
"""
🚀 GEDEON API - VERSION OPTIMISÉE v2

Optimisations :
1. DATAtourisme + OpenAgenda en PARALLÈLE
2. Mapping statique des départements Allociné (pas d'appel API au démarrage)
3. Recherche cinéma par code postal (plus fiable)
4. Recherche élargie IDF et départements adjacents
5. Cache persistant des coordonnées de cinémas
6. Utilisation de get_movies() pour données enrichies (poster, genres, etc.)
"""

from flask import Flask, request, jsonify, send_from_directory, redirect
from flask_cors import CORS
from datetime import datetime, timezone, timedelta, date
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
import re
from urllib.parse import urlparse
import requests
import math
import time
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from werkzeug.security import generate_password_hash, check_password_hash

# Module d'authentification email
try:
    from auth_email import (
        generate_confirmation_token,
        send_confirmation_email,
        send_password_reset_email,
        is_valid_email,
        is_token_expired,
        TOKEN_EXPIRY_HOURS
    )
    AUTH_EMAIL_AVAILABLE = True
    print("✅ Module auth_email disponible")
except ImportError:
    AUTH_EMAIL_AVAILABLE = False
    print("⚠️ Module auth_email non disponible")

# ============================================================================
# IMPORT DES MODULES OPTIMISÉS
# ============================================================================

# Mapping départements
from department_mapping import (
    get_allocine_dept_id,
    get_allocine_dept_id_from_postcode,
    get_all_dept_ids_for_location,
    is_in_idf,
    IDF_DEPARTMENTS,
    POSTCODE_TO_ALLOCINE,
    ADJACENT_DEPARTMENTS
)

# Allociné API
try:
    from allocineAPI.allocineAPI import allocineAPI
    ALLOCINE_AVAILABLE = True
    print("✅ Allociné API disponible")
except ImportError:
    ALLOCINE_AVAILABLE = False
    print("⚠️ Allociné API non disponible")

# ============================================================================
# PERSISTENT STORAGE CONFIGURATION
# ============================================================================

# Use Render Persistent Disk if available, fallback to relative path
PERSISTENT_DISK_PATH = os.environ.get('PERSISTENT_DISK_PATH', '/mnt/data')

# Determine uploads base directory
if os.path.exists(PERSISTENT_DISK_PATH):
    UPLOADS_BASE_DIR = PERSISTENT_DISK_PATH
    print(f"✅ Using persistent disk: {UPLOADS_BASE_DIR}")
else:
    UPLOADS_BASE_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
    print(f"⚠️  Using relative path (ephemeral): {UPLOADS_BASE_DIR}")
    print(f"   💡 Set PERSISTENT_DISK_PATH env var to use persistent storage")

# ============================================================================
# MAPPING DYNAMIQUE ALLOCINÉ (chargé au démarrage)
# ============================================================================

ALLOCINE_DEPT_MAPPING = {}  # nom_normalisé → id_allocine
ALLOCINE_DEPT_MAPPING_LOADED = False

def load_allocine_departments():
    """
    Charge le mapping des départements depuis l'API Allociné.
    Appelé une fois au démarrage du serveur.
    """
    global ALLOCINE_DEPT_MAPPING, ALLOCINE_DEPT_MAPPING_LOADED
    
    if not ALLOCINE_AVAILABLE:
        print("   ⚠️ Allociné non disponible, mapping non chargé")
        return
    
    if ALLOCINE_DEPT_MAPPING_LOADED:
        return
    
    try:
        print("   🔄 Chargement des départements Allociné...")
        api = allocineAPI()
        depts = api.get_departements()
        
        for dept in depts:
            name = dept.get('name', '')
            dept_id = dept.get('id', '')
            
            if name and dept_id:
                # Normaliser le nom (minuscules, sans accents problématiques)
                name_normalized = name.lower().strip()
                ALLOCINE_DEPT_MAPPING[name_normalized] = dept_id
                
                # Ajouter des variantes sans tirets/accents
                name_simple = name_normalized.replace('-', ' ').replace("'", " ")
                if name_simple != name_normalized:
                    ALLOCINE_DEPT_MAPPING[name_simple] = dept_id
        
        ALLOCINE_DEPT_MAPPING_LOADED = True
        print(f"   ✅ {len(depts)} départements Allociné chargés")
        
        # Afficher quelques exemples pour debug
        examples = list(ALLOCINE_DEPT_MAPPING.items())[:5]
        for name, dept_id in examples:
            print(f"      '{name}' → {dept_id}")
        
    except Exception as e:
        print(f"   ❌ Erreur chargement départements Allociné: {e}")
        import traceback
        traceback.print_exc()


def get_allocine_dept_id_dynamic(dept_name):
    """
    Récupère l'ID Allociné pour un nom de département.
    Utilise le mapping dynamique chargé au démarrage.
    """
    if not ALLOCINE_DEPT_MAPPING:
        load_allocine_departments()
    
    if not dept_name:
        return None
    
    # Normaliser le nom recherché
    name_normalized = dept_name.lower().strip()
    
    # Recherche exacte
    if name_normalized in ALLOCINE_DEPT_MAPPING:
        return ALLOCINE_DEPT_MAPPING[name_normalized]
    
    # Recherche sans tirets
    name_simple = name_normalized.replace('-', ' ').replace("'", " ")
    if name_simple in ALLOCINE_DEPT_MAPPING:
        return ALLOCINE_DEPT_MAPPING[name_simple]
    
    # Recherche partielle (le nom contient ou est contenu)
    for key, value in ALLOCINE_DEPT_MAPPING.items():
        if key in name_normalized or name_normalized in key:
            return value
    
    return None


# ============================================================================
# CONFIGURATION
# ============================================================================

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

# PostgreSQL
database_url = os.environ.get('DATABASE_URL_RENDER') or os.environ.get('DATABASE_URL')

if database_url:
    url = urlparse(database_url)
    DB_CONFIG = {
        'host': url.hostname,
        'port': url.port or 5432,
        'database': url.path[1:],
        'user': url.username,
        'password': url.password,
        'sslmode': 'require'
    }
    print(f"✅ Connexion à Render: {url.hostname}")
else:
    DB_CONFIG = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': int(os.environ.get('DB_PORT', 5432)),
        'database': os.environ.get('DB_NAME', 'datatourisme'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'sslmode': 'prefer'
    }
    print(f"⚠️ Connexion locale: {DB_CONFIG['host']}")

# OpenAgenda
API_KEY = os.environ.get("OPENAGENDA_API_KEY", "")
BASE_URL = os.environ.get("OPENAGENDA_BASE_URL", "https://api.openagenda.com/v2")

# Valeurs par défaut
RADIUS_KM_DEFAULT = 30
DAYS_AHEAD_DEFAULT = 30

# Caches
GEOCODE_CACHE = {}
CINEMA_COORDS_CACHE = {}
CINEMA_CACHE_FILE = "/tmp/allocine_cinemas_coords.pkl"
CINEMAS_BY_DEPT_CACHE = {}
CINEMAS_CACHE_TIMESTAMPS = {}
CINEMAS_CACHE_DURATION = 3600 * 6  # 6 heures


# ============================================================================
# INITIALISATION DES TABLES USERS & SCANNED_EVENTS
# ============================================================================

def init_user_tables():
    """
    Crée les tables users et scanned_events si elles n'existent pas.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Table users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                pseudo VARCHAR(20) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                email_confirmed BOOLEAN DEFAULT FALSE,
                confirmation_token VARCHAR(64),
                confirmation_sent_at TIMESTAMP,
                reset_token VARCHAR(64),
                reset_sent_at TIMESTAMP,
                device_id VARCHAR(64),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Migration : ajouter les nouvelles colonnes si elles n'existent pas
        cur.execute("""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='email') THEN
                    ALTER TABLE users ADD COLUMN email VARCHAR(255);
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='email_confirmed') THEN
                    ALTER TABLE users ADD COLUMN email_confirmed BOOLEAN DEFAULT FALSE;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='confirmation_token') THEN
                    ALTER TABLE users ADD COLUMN confirmation_token VARCHAR(64);
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='confirmation_sent_at') THEN
                    ALTER TABLE users ADD COLUMN confirmation_sent_at TIMESTAMP;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='reset_token') THEN
                    ALTER TABLE users ADD COLUMN reset_token VARCHAR(64);
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='reset_sent_at') THEN
                    ALTER TABLE users ADD COLUMN reset_sent_at TIMESTAMP;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='password_hash') THEN
                    ALTER TABLE users ADD COLUMN password_hash VARCHAR(255);
                END IF;
            END $$;
        """)
        
        # Index pour recherche rapide
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_pseudo ON users(pseudo)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_confirmation_token ON users(confirmation_token)
        """)
        
        # Table scanned_events
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scanned_events (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                uid VARCHAR(100) UNIQUE NOT NULL,
                title VARCHAR(500),
                category VARCHAR(100),
                begin_date DATE,
                end_date DATE,
                start_time VARCHAR(10),
                end_time VARCHAR(10),
                location_name VARCHAR(500),
                city VARCHAR(200),
                address VARCHAR(500),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                description TEXT,
                organizer VARCHAR(500),
                pricing VARCHAR(200),
                website VARCHAR(500),
                tags TEXT[],
                is_private BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Ajouter colonne website si elle n'existe pas (migration)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanned_events' AND column_name='website') THEN
                    ALTER TABLE scanned_events ADD COLUMN website VARCHAR(500);
                END IF;
            END $$;
        """)

        # Ajouter colonne image_path si elle n'existe pas (migration)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanned_events' AND column_name='image_path') THEN
                    ALTER TABLE scanned_events ADD COLUMN image_path VARCHAR(500);
                END IF;
            END $$;
        """)
        
        # Index pour recherche géographique
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_scanned_events_user ON scanned_events(user_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_scanned_events_private ON scanned_events(is_private)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_scanned_events_coords ON scanned_events(latitude, longitude)
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print("✅ Tables users et scanned_events initialisées")
        return True
        
    except Exception as e:
        print(f"⚠️ Erreur init tables users/scanned: {e}")
        return False


# Variable pour savoir si les tables ont été initialisées
USER_TABLES_INITIALIZED = False


# Bounding boxes approximatives des départements français (lat_min, lat_max, lon_min, lon_max)
# Utilisé pour vérifier la cohérence des résultats Nominatim
DEPT_BOUNDING_BOXES = {
    '01': (45.6, 46.5, 4.7, 6.2),    # Ain
    '02': (49.0, 50.1, 3.0, 4.3),    # Aisne
    '03': (46.0, 46.8, 2.3, 4.0),    # Allier
    '04': (43.7, 44.7, 5.5, 6.9),    # Alpes-de-Haute-Provence
    '05': (44.2, 45.1, 5.4, 6.9),    # Hautes-Alpes
    '06': (43.5, 44.4, 6.6, 7.7),    # Alpes-Maritimes
    '07': (44.3, 45.4, 3.9, 4.9),    # Ardèche
    '08': (49.2, 50.2, 4.0, 5.4),    # Ardennes
    '09': (42.6, 43.3, 0.8, 2.2),    # Ariège
    '10': (47.9, 48.7, 3.4, 4.9),    # Aube
    '11': (42.7, 43.5, 1.7, 3.2),    # Aude
    '12': (43.7, 44.9, 1.8, 3.5),    # Aveyron
    '13': (43.2, 43.9, 4.2, 5.8),    # Bouches-du-Rhône
    '14': (48.8, 49.4, -1.2, 0.4),   # Calvados
    '15': (44.6, 45.5, 2.1, 3.4),    # Cantal
    '16': (45.2, 46.1, -0.5, 0.6),   # Charente
    '17': (45.1, 46.4, -1.5, -0.1),  # Charente-Maritime
    '18': (46.4, 47.6, 1.8, 3.1),    # Cher
    '19': (44.9, 45.8, 1.2, 2.5),    # Corrèze
    '21': (46.9, 48.0, 4.1, 5.5),    # Côte-d'Or
    '22': (48.3, 48.9, -3.7, -1.9),  # Côtes-d'Armor
    '23': (45.7, 46.4, 1.4, 2.6),    # Creuse
    '24': (44.5, 45.7, -0.1, 1.5),   # Dordogne
    '25': (46.6, 47.6, 5.7, 7.1),    # Doubs
    '26': (44.1, 45.4, 4.6, 5.8),    # Drôme
    '27': (48.7, 49.5, 0.3, 1.8),    # Eure
    '28': (47.9, 48.7, 0.8, 2.0),    # Eure-et-Loir
    '29': (47.7, 48.8, -5.2, -3.4),  # Finistère
    '30': (43.5, 44.5, 3.3, 4.8),    # Gard
    '31': (42.9, 43.9, 0.4, 2.0),    # Haute-Garonne
    '32': (43.3, 44.1, -0.3, 1.2),   # Gers
    '33': (44.2, 45.6, -1.3, 0.3),   # Gironde
    '34': (43.2, 43.9, 2.5, 4.2),    # Hérault
    '35': (47.6, 48.6, -2.3, -1.0),  # Ille-et-Vilaine
    '36': (46.3, 47.2, 0.9, 2.2),    # Indre
    '37': (46.7, 47.7, 0.0, 1.4),    # Indre-et-Loire
    '38': (44.7, 45.9, 4.7, 6.4),    # Isère
    '39': (46.3, 47.2, 5.3, 6.2),    # Jura
    '40': (43.5, 44.5, -1.5, 0.1),   # Landes
    '41': (47.2, 48.1, 0.6, 2.2),    # Loir-et-Cher
    '42': (45.3, 46.3, 3.7, 4.8),    # Loire
    '43': (44.7, 45.4, 3.1, 4.5),    # Haute-Loire
    '44': (46.9, 47.8, -2.6, -1.0),  # Loire-Atlantique
    '45': (47.5, 48.3, 1.5, 3.1),    # Loiret
    '46': (44.2, 45.1, 1.0, 2.2),    # Lot
    '47': (43.8, 44.8, -0.2, 1.1),   # Lot-et-Garonne
    '48': (44.1, 44.9, 2.9, 4.0),    # Lozère
    '49': (47.0, 47.8, -1.4, 0.2),   # Maine-et-Loire
    '50': (48.5, 49.7, -2.0, -0.8),  # Manche
    '51': (48.5, 49.4, 3.4, 5.0),    # Marne
    '52': (47.6, 48.7, 4.6, 5.9),    # Haute-Marne
    '53': (47.7, 48.5, -1.2, 0.0),   # Mayenne
    '54': (48.3, 49.2, 5.4, 7.1),    # Meurthe-et-Moselle
    '55': (48.4, 49.4, 4.9, 5.9),    # Meuse
    '56': (47.3, 48.0, -3.7, -2.0),  # Morbihan
    '57': (48.6, 49.5, 5.9, 7.6),    # Moselle
    '58': (46.7, 47.6, 2.8, 4.2),    # Nièvre
    '59': (50.0, 51.1, 2.1, 4.2),    # Nord
    '60': (49.1, 49.8, 1.7, 3.2),    # Oise
    '61': (48.4, 48.9, -0.9, 0.9),   # Orne
    '62': (50.0, 51.0, 1.5, 3.2),    # Pas-de-Calais
    '63': (45.3, 46.3, 2.4, 3.9),    # Puy-de-Dôme
    '64': (42.8, 43.6, -1.8, 0.0),   # Pyrénées-Atlantiques
    '65': (42.7, 43.4, -0.4, 0.6),   # Hautes-Pyrénées
    '66': (42.3, 42.9, 1.7, 3.2),    # Pyrénées-Orientales
    '67': (48.1, 49.1, 7.0, 8.2),    # Bas-Rhin
    '68': (47.4, 48.3, 6.8, 7.6),    # Haut-Rhin
    '69': (45.5, 46.3, 4.2, 5.2),    # Rhône
    '70': (47.3, 48.0, 5.4, 6.8),    # Haute-Saône
    '71': (46.2, 47.2, 3.6, 5.1),    # Saône-et-Loire
    '72': (47.6, 48.5, -0.5, 0.9),   # Sarthe
    '73': (45.1, 45.9, 5.6, 7.2),    # Savoie
    '74': (45.7, 46.5, 5.8, 7.0),    # Haute-Savoie
    '75': (48.8, 48.9, 2.2, 2.5),    # Paris
    '76': (49.2, 50.1, 0.1, 1.8),    # Seine-Maritime
    '77': (48.1, 49.1, 2.4, 3.6),    # Seine-et-Marne
    '78': (48.4, 49.1, 1.4, 2.2),    # Yvelines
    '79': (46.0, 47.1, -0.9, 0.2),   # Deux-Sèvres
    '80': (49.6, 50.4, 1.4, 3.2),    # Somme
    '81': (43.4, 44.2, 1.5, 2.9),    # Tarn
    '82': (43.8, 44.4, 0.7, 2.0),    # Tarn-et-Garonne
    '83': (43.0, 43.8, 5.7, 6.9),    # Var
    '84': (43.7, 44.4, 4.6, 5.8),    # Vaucluse
    '85': (46.3, 47.1, -2.4, -0.6),  # Vendée
    '86': (46.0, 47.2, -0.1, 1.2),   # Vienne
    '87': (45.4, 46.4, 0.6, 1.9),    # Haute-Vienne
    '88': (47.8, 48.5, 5.5, 7.2),    # Vosges
    '89': (47.3, 48.4, 2.8, 4.3),    # Yonne
    '90': (47.4, 47.8, 6.7, 7.2),    # Territoire de Belfort
    '91': (48.3, 48.8, 2.0, 2.6),    # Essonne
    '92': (48.7, 48.9, 2.1, 2.4),    # Hauts-de-Seine
    '93': (48.8, 49.0, 2.3, 2.6),    # Seine-Saint-Denis
    '94': (48.7, 48.9, 2.3, 2.6),    # Val-de-Marne
    '95': (48.9, 49.2, 1.6, 2.6),    # Val-d'Oise
    '2A': (41.3, 42.0, 8.5, 9.4),    # Corse-du-Sud
    '2B': (42.0, 43.0, 9.0, 9.6),    # Haute-Corse
}


def is_coords_in_dept(lat, lon, dept_code):
    """
    Vérifie si les coordonnées sont cohérentes avec le département.
    Utilisé pour filtrer les résultats Nominatim aberrants.
    
    Returns: True si cohérent ou si dept_code inconnu, False sinon
    """
    if not dept_code or not lat or not lon:
        return True  # Pas de vérification possible
    
    # Normaliser le code département
    dept = str(dept_code).upper().zfill(2)
    
    if dept not in DEPT_BOUNDING_BOXES:
        return True  # Département inconnu, on accepte
    
    lat_min, lat_max, lon_min, lon_max = DEPT_BOUNDING_BOXES[dept]
    
    # Ajouter une marge de 0.5° (~50km) pour les cas limites
    margin = 0.5
    
    in_box = (lat_min - margin <= lat <= lat_max + margin and 
              lon_min - margin <= lon <= lon_max + margin)
    
    return in_box

# ============================================================================
# BASE DE DONNÉES CNC DES CINÉMAS FRANÇAIS (avec GPS)
# ============================================================================

CINEMAS_CNC_DATA = []  # Liste des cinémas avec coordonnées GPS
CINEMAS_CNC_LOADED = False

def load_cinemas_cnc():
    """
    Charge la base de données CNC des cinémas français avec coordonnées GPS.
    Fichier généré depuis Données_cartographie_2024.xlsx du CNC.
    """
    global CINEMAS_CNC_DATA, CINEMAS_CNC_LOADED
    
    if CINEMAS_CNC_LOADED:
        return
    
    import json
    cnc_file = os.path.join(os.path.dirname(__file__), 'cinemas_france_data.json')
    
    if os.path.exists(cnc_file):
        try:
            with open(cnc_file, 'r', encoding='utf-8') as f:
                CINEMAS_CNC_DATA = json.load(f)
            CINEMAS_CNC_LOADED = True
            print(f"   ✅ Base CNC chargée: {len(CINEMAS_CNC_DATA)} cinémas avec GPS")
        except Exception as e:
            print(f"   ⚠️ Erreur chargement base CNC: {e}")
    else:
        print(f"   ⚠️ Fichier CNC non trouvé: {cnc_file}")


def find_cinema_gps_cnc(cinema_name, cinema_address=None, dept_code=None):
    """
    Recherche les coordonnées GPS d'un cinéma dans la base CNC.
    Utilise une recherche fuzzy basée sur les mots-clés du nom.
    Prend en compte le département pour éviter les homonymes.
    
    Args:
        cinema_name: Nom du cinéma (ex: "Le Travelling")
        cinema_address: Adresse complète (ex: "... 34300 Agde")
        dept_code: Code département (ex: "34")
    
    Returns: (lat, lon) ou (None, None)
    """
    if not CINEMAS_CNC_DATA:
        load_cinemas_cnc()
    
    if not CINEMAS_CNC_DATA:
        return None, None
    
    import re
    
    # Normaliser le nom recherché
    name_normalized = cinema_name.lower().strip()
    name_normalized = re.sub(r'\s+', ' ', name_normalized)
    
    # Extraire les mots-clés du nom recherché
    search_keywords = set(re.findall(r'[a-zàâäéèêëïîôùûüç0-9]+', name_normalized))
    search_keywords.discard('le')
    search_keywords.discard('la')
    search_keywords.discard('les')
    search_keywords.discard('du')
    search_keywords.discard('de')
    search_keywords.discard('des')
    search_keywords.discard('cinema')
    search_keywords.discard('cinéma')
    search_keywords.discard('cine')
    search_keywords.discard('ciné')
    
    # Extraire le département et la ville de l'adresse si disponible
    search_dept = dept_code
    search_commune = None
    if cinema_address:
        # Chercher le code postal et la ville
        match = re.search(r'(\d{5})\s+([A-Za-zÀ-ÿ\-\' ]+)$', cinema_address)
        if match:
            cp = match.group(1)
            search_commune = match.group(2).lower().strip()
            # Extraire le département du code postal
            if not search_dept:
                if cp.upper().startswith('2A') or cp.upper().startswith('2B'):
                    search_dept = cp[:2].upper()
                else:
                    search_dept = cp[:2]
    
    best_match = None
    best_score = 0
    
    for cinema in CINEMAS_CNC_DATA:
        cinema_keywords = set(cinema.get('keywords', []))
        
        # Score basé sur les mots-clés communs
        common_keywords = search_keywords & cinema_keywords
        
        if not common_keywords:
            continue
        
        score = len(common_keywords) * 10
        
        # Bonus si le nom correspond exactement
        if name_normalized == cinema['nom_normalized']:
            score += 100
        elif name_normalized in cinema['nom_normalized'] or cinema['nom_normalized'] in name_normalized:
            score += 50
        
        # IMPORTANT: Bonus si le département correspond (évite les homonymes)
        if search_dept and cinema.get('dept') == search_dept:
            score += 200  # Priorité forte au département
        
        # Bonus si la commune correspond
        if search_commune and search_commune in cinema['commune_normalized']:
            score += 100
        
        if score > best_score:
            best_score = score
            best_match = cinema
    
    if best_match and best_score >= 10:
        return best_match['lat'], best_match['lon']
    
    return None, None

# OpenAgenda
OPENAGENDA_MAX_WORKERS = 10
OPENAGENDA_AGENDAS_LIMIT = 30
OPENAGENDA_EVENTS_PER_AGENDA = 30
OPENAGENDA_CACHE_FILE = "/tmp/openagenda_agendas_cache.pkl"
OPENAGENDA_CACHE_DURATION = timedelta(hours=24)

# Coordonnées connues de cinémas
KNOWN_CINEMAS_GPS = {
    'ugc ciné cité les halles': (48.8619, 2.3466),
    'pathé beaugrenelle': (48.8478, 2.2820),
    'mk2 bibliothèque': (48.8338, 2.3761),
    'mk2 quai de seine': (48.8840, 2.3719),
    'gaumont champs-élysées': (48.8698, 2.3046),
    'gaumont opéra': (48.8716, 2.3315),
    'ugc montparnasse': (48.8422, 2.3244),
    'le grand rex': (48.8707, 2.3477),
    'pathé la villette': (48.8938, 2.3889),
    'pathé levallois': (48.8920, 2.2883),
    'ugc ciné cité la défense': (48.8920, 2.2380),
    'pathé bellecour': (45.7578, 4.8320),
    'pathé madeleine': (43.2965, 5.3698),
    'gaumont wilson': (43.6070, 1.4480),
}


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def get_db_connection():
    """Crée une connexion à PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def haversine_km(lat1, lon1, lat2, lon2):
    """Distance en km entre deux points GPS."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def calculate_bounding_box(lat, lng, radius_km):
    """Calcule la bounding box pour une recherche géographique."""
    EARTH_RADIUS_KM = 6371.0
    radius_rad = radius_km / EARTH_RADIUS_KM
    lat_rad = math.radians(lat)
    lat_delta = math.degrees(radius_rad)
    lng_delta = math.degrees(radius_rad / math.cos(lat_rad))
    return {
        'northEast': {'lat': lat + lat_delta, 'lng': lng + lng_delta},
        'southWest': {'lat': lat - lat_delta, 'lng': lng - lng_delta}
    }


def reverse_geocode_nominatim(lat, lon):
    """
    Récupère les infos de localisation via Nominatim.
    Retourne: (dept_name, postcode, city)
    """
    # Cache avec précision à 3 décimales (~100m) au lieu de 2 (~1km)
    cache_key = (round(lat, 3), round(lon, 3))
    if cache_key in GEOCODE_CACHE:
        cached = GEOCODE_CACHE[cache_key]
        # Vérifier que c'est bien un tuple de 3 éléments (pas un ancien format)
        if isinstance(cached, tuple) and len(cached) == 3:
            return cached
    
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json", "zoom": 10, "addressdetails": 1}
    headers = {"User-Agent": "gedeon-events-api/1.0"}
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        address = data.get("address", {})
        
        postcode = address.get("postcode", "")
        city = address.get("city") or address.get("town") or address.get("village") or ""
        county = address.get("county", "")
        state = address.get("state", "")
        
        if city in ["Paris", "Lyon", "Marseille"]:
            dept_name = city
        elif county:
            dept_name = county
        else:
            dept_name = state
        
        result = (dept_name, postcode, city)
        GEOCODE_CACHE[cache_key] = result
        return result
        
    except Exception as e:
        print(f"   ⚠️ Erreur Nominatim reverse: {e}")
        GEOCODE_CACHE[cache_key] = (None, None, None)
        return (None, None, None)


def geocode_address_nominatim(address_str):
    """Géocode une adresse texte avec respect du rate limit Nominatim."""
    if not address_str:
        return None, None
    
    if address_str in GEOCODE_CACHE:
        cached = GEOCODE_CACHE[address_str]
        if isinstance(cached, tuple) and len(cached) == 2:
            return cached
    
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address_str, "format": "json", "limit": 1}
    headers = {"User-Agent": "gedeon-events-api/1.0"}
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data:
            lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
            GEOCODE_CACHE[address_str] = (lat, lon)
            time.sleep(0.05)  # 50ms entre requêtes (respect rate limit Nominatim)
            return lat, lon
    except Exception:
        pass
    
    GEOCODE_CACHE[address_str] = (None, None)
    return None, None


def load_cinema_coords_cache():
    """Charge le cache des coordonnées de cinémas."""
    global CINEMA_COORDS_CACHE
    if os.path.exists(CINEMA_CACHE_FILE):
        try:
            with open(CINEMA_CACHE_FILE, 'rb') as f:
                CINEMA_COORDS_CACHE = pickle.load(f)
                print(f"   💾 Cache cinémas: {len(CINEMA_COORDS_CACHE)} entrées")
        except Exception:
            pass


def save_cinema_coords_cache():
    """Sauvegarde le cache des coordonnées."""
    try:
        with open(CINEMA_CACHE_FILE, 'wb') as f:
            pickle.dump(CINEMA_COORDS_CACHE, f)
    except Exception:
        pass


# ============================================================================
# DATATOURISME
# ============================================================================

def fetch_datatourisme_events(center_lat, center_lon, radius_km, days_ahead):
    """Récupère les événements DATAtourisme (requête SQL optimisée)."""
    try:
        start_time = time.time()
        conn = get_db_connection()
        cur = conn.cursor()
        
        date_limite = datetime.now().date() + timedelta(days=days_ahead)
        
        # Requête corrigée : 
        # - date_fin >= aujourd'hui (événement pas encore terminé)
        # - date_debut <= date_limite (événement commence dans la période)
        # - Si date_fin est NULL, on utilise date_debut >= aujourd'hui
        query = """
            WITH nearby_events AS (
                SELECT uri, nom, description, date_debut, date_fin,
                       latitude, longitude, adresse, commune, code_postal, contacts, geom
                FROM evenements
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
                  AND (
                      -- Événement avec date de fin : doit être >= aujourd'hui
                      (date_fin IS NOT NULL AND date_fin >= CURRENT_DATE AND date_debut <= %s)
                      OR
                      -- Événement sans date de fin : date_debut doit être >= aujourd'hui et <= limite
                      (date_fin IS NULL AND date_debut >= CURRENT_DATE AND date_debut <= %s)
                  )
                LIMIT 500
            )
            SELECT uri as uid, nom as title, description,
                   date_debut as begin, date_fin as end,
                   latitude, longitude, adresse as address, commune as city,
                   code_postal as "postalCode", contacts,
                   ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000 as "distanceKm"
            FROM nearby_events
            ORDER BY "distanceKm", date_debut
        """
        
        cur.execute(query, (center_lon, center_lat, radius_km * 1000, date_limite, date_limite, center_lon, center_lat))
        rows = cur.fetchall()
        
        events = []
        for row in rows:
            event = dict(row)
            if event.get('begin'):
                event['begin'] = event['begin'].isoformat()
            if event.get('end'):
                event['end'] = event['end'].isoformat()
            if event.get('distanceKm'):
                event['distanceKm'] = round(event['distanceKm'], 1)
            
            event['locationName'] = event.get('city', '')
            event['source'] = 'DATAtourisme'
            event['agendaTitle'] = 'DATAtourisme'
            
            contacts = event.get('contacts', '')
            event['openagendaUrl'] = ''
            if contacts and '#' in contacts:
                for part in contacts.split('#'):
                    if part.startswith('http'):
                        event['openagendaUrl'] = part
                        break
            
            events.append(event)
        
        cur.close()
        conn.close()
        
        print(f"   ⚡ DATAtourisme: {len(events)} événements en {time.time()-start_time:.3f}s")
        return events
        
    except Exception as e:
        print(f"   ❌ Erreur DATAtourisme: {e}")
        return []


# ============================================================================
# OPENAGENDA
# ============================================================================

def get_cached_agendas():
    """Cache la liste des agendas pendant 24h."""
    if os.path.exists(OPENAGENDA_CACHE_FILE):
        try:
            with open(OPENAGENDA_CACHE_FILE, 'rb') as f:
                cached_data = pickle.load(f)
                if datetime.now() - cached_data['timestamp'] < OPENAGENDA_CACHE_DURATION:
                    return cached_data['agendas']
        except Exception:
            pass
    
    if not API_KEY:
        return []
    
    url = f"{BASE_URL}/agendas"
    params = {"key": API_KEY, "size": 100}
    
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        agendas = r.json().get('agendas', [])
        
        with open(OPENAGENDA_CACHE_FILE, 'wb') as f:
            pickle.dump({'timestamp': datetime.now(), 'agendas': agendas}, f)
        
        return agendas
    except Exception:
        return []


def process_agenda_events(agenda, center_lat, center_lon, radius_km, days_ahead):
    """Worker pour traiter un agenda OpenAgenda."""
    uid = agenda.get('uid')
    agenda_slug = agenda.get('slug')
    title = agenda.get('title', {})
    agenda_title = title.get('fr') or title.get('en') or 'Agenda' if isinstance(title, dict) else (title or 'Agenda')
    
    try:
        url = f"{BASE_URL}/agendas/{uid}/events"
        bbox = calculate_bounding_box(center_lat, center_lon, radius_km)
        today_str = datetime.now().strftime('%Y-%m-%d')
        end_date_str = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
        
        params = {
            'key': API_KEY, 'size': OPENAGENDA_EVENTS_PER_AGENDA, 'detailed': 1,
            'geo[northEast][lat]': bbox['northEast']['lat'],
            'geo[northEast][lng]': bbox['northEast']['lng'],
            'geo[southWest][lat]': bbox['southWest']['lat'],
            'geo[southWest][lng]': bbox['southWest']['lng'],
            'timings[gte]': today_str, 'timings[lte]': end_date_str,
        }
        
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        events = r.json().get('events', [])
        
        if not events:
            return []
        
        agenda_events = []
        for ev in events:
            timings = ev.get('timings') or []
            
            # Trouver le premier timing futur
            begin_str = None
            end_str = None
            today = datetime.now().date()
            
            for timing in timings:
                timing_begin = timing.get('begin')
                if timing_begin:
                    try:
                        timing_date = datetime.fromisoformat(timing_begin.replace('Z', '+00:00')).date()
                        if timing_date >= today:
                            begin_str = timing_begin
                            end_str = timing.get('end')
                            break
                    except:
                        pass
            
            # Si aucun timing futur, ignorer cet événement
            if not begin_str:
                continue
            
            loc = ev.get('location') or {}
            ev_lat, ev_lon = loc.get('latitude'), loc.get('longitude')
            
            if ev_lat is None or ev_lon is None:
                parts = [loc.get("name"), loc.get("address"), loc.get("city"), "France"]
                address_str = ", ".join([p for p in parts if p])
                ev_lat, ev_lon = geocode_address_nominatim(address_str)
                if ev_lat is None:
                    continue
                time.sleep(0.1)
            
            try:
                ev_lat, ev_lon = float(ev_lat), float(ev_lon)
            except (ValueError, TypeError):
                continue
            
            dist = haversine_km(center_lat, center_lon, ev_lat, ev_lon)
            if dist > radius_km:
                continue
            
            title_field = ev.get('title')
            ev_title = title_field.get('fr') or title_field.get('en') or 'Événement' if isinstance(title_field, dict) else (title_field or 'Événement')
            
            event_slug = ev.get('slug')
            openagenda_url = f"https://openagenda.com/{agenda_slug}/events/{event_slug}?lang=fr" if agenda_slug and event_slug else None
            
            agenda_events.append({
                "uid": f"oa-{ev.get('uid')}",
                "title": ev_title,
                "begin": begin_str,
                "end": end_str,
                "locationName": loc.get("name"),
                "city": loc.get("city"),
                "address": loc.get("address"),
                "latitude": ev_lat,
                "longitude": ev_lon,
                "distanceKm": round(dist, 1),
                "openagendaUrl": openagenda_url,
                "agendaTitle": agenda_title,
                "source": "OpenAgenda"
            })
        
        return agenda_events
        
    except Exception:
        return []


def fetch_openagenda_events(center_lat, center_lon, radius_km, days_ahead):
    """Récupère les événements OpenAgenda avec parallélisation."""
    start_time = time.time()
    
    agendas = get_cached_agendas()
    if not agendas:
        return []
    
    # Sélectionner les meilleurs agendas
    official = [a for a in agendas if a.get('official')]
    others = [a for a in agendas if not a.get('official')]
    top_agendas = official[:20] + others[:10]
    
    all_events = []
    
    with ThreadPoolExecutor(max_workers=OPENAGENDA_MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_agenda_events, agenda, center_lat, center_lon, radius_km, days_ahead): agenda
            for agenda in top_agendas
        }
        
        for future in as_completed(futures):
            try:
                events = future.result(timeout=20)
                all_events.extend(events)
            except Exception:
                pass
    
    print(f"   ⚡ OpenAgenda: {len(all_events)} événements en {time.time()-start_time:.1f}s")
    return all_events


# ============================================================================
# ALLOCINÉ OPTIMISÉ
# ============================================================================

def get_cinemas_for_department(dept_id):
    """Récupère les cinémas d'un département avec cache."""
    if not ALLOCINE_AVAILABLE:
        return []
    
    now = time.time()
    if dept_id in CINEMAS_BY_DEPT_CACHE:
        if now - CINEMAS_CACHE_TIMESTAMPS.get(dept_id, 0) < CINEMAS_CACHE_DURATION:
            return CINEMAS_BY_DEPT_CACHE[dept_id]
    
    try:
        api = allocineAPI()
        cinemas = api.get_cinema(dept_id)
        CINEMAS_BY_DEPT_CACHE[dept_id] = cinemas
        CINEMAS_CACHE_TIMESTAMPS[dept_id] = now
        return cinemas
    except Exception as e:
        print(f"   ⚠️ Erreur get_cinema({dept_id}): {e}")
        return []


def geocode_cinema(cinema_name, cinema_address, dept_code=None):
    """
    Géocode un cinéma avec priorité:
    1. Cache local
    2. Base CNC (2053 cinémas français avec GPS)
    3. Coordonnées connues (fallback)
    4. Nominatim (dernier recours) - avec vérification de cohérence
    
    Args:
        cinema_name: Nom du cinéma
        cinema_address: Adresse du cinéma
        dept_code: Code département (ex: "34") pour éviter les homonymes
    """
    cache_key = f"{cinema_name}:{cinema_address}:{dept_code}"
    if cache_key in CINEMA_COORDS_CACHE:
        return CINEMA_COORDS_CACHE[cache_key]
    
    # 1. Chercher dans la base CNC (instantané) - avec département
    lat, lon = find_cinema_gps_cnc(cinema_name, cinema_address, dept_code)
    if lat and lon:
        CINEMA_COORDS_CACHE[cache_key] = (lat, lon)
        return (lat, lon)
    
    # 2. Coordonnées connues (fallback manuel)
    name_lower = cinema_name.lower().strip()
    for known_name, coords in KNOWN_CINEMAS_GPS.items():
        if known_name in name_lower or name_lower.startswith(known_name[:10]):
            CINEMA_COORDS_CACHE[cache_key] = coords
            return coords
    
    # 3. Géocodage Nominatim (dernier recours - plus lent)
    import re
    
    if cinema_address:
        # Stratégie 1: Adresse complète
        lat, lon = geocode_address_nominatim(f"{cinema_address}, France")
        if lat and is_coords_in_dept(lat, lon, dept_code):
            CINEMA_COORDS_CACHE[cache_key] = (lat, lon)
            save_cinema_coords_cache()
            return (lat, lon)
        
        # Stratégie 2: Extraire code postal et ville de l'adresse
        match = re.search(r'(\d{5})\s+([A-Za-zÀ-ÿ\-\' ]+)$', cinema_address)
        if match:
            cp, ville = match.groups()
            simplified = f"{ville.strip()}, {cp}, France"
            lat, lon = geocode_address_nominatim(simplified)
            if lat and is_coords_in_dept(lat, lon, dept_code):
                CINEMA_COORDS_CACHE[cache_key] = (lat, lon)
                save_cinema_coords_cache()
                return (lat, lon)
        
        # Stratégie 3: Juste le code postal (centre de la commune)
        match_cp = re.search(r'(\d{5})', cinema_address)
        if match_cp:
            cp = match_cp.group(1)
            lat, lon = geocode_address_nominatim(f"{cp}, France")
            if lat and is_coords_in_dept(lat, lon, dept_code):
                CINEMA_COORDS_CACHE[cache_key] = (lat, lon)
                save_cinema_coords_cache()
                return (lat, lon)
    
    CINEMA_COORDS_CACHE[cache_key] = (None, None)
    return (None, None)


def fetch_movies_for_cinema(cinema_info, today_str, tomorrow_str=None):
    """Worker pour récupérer les films d'un cinéma (aujourd'hui + demain)."""
    try:
        api = allocineAPI()
        cinema_id = cinema_info['id']
        
        all_movies = {}  # {title: movie_data} pour dédupliquer
        
        # Récupérer les séances d'aujourd'hui
        for date_str in [today_str, tomorrow_str] if tomorrow_str else [today_str]:
            try:
                showtimes = api.get_showtime(cinema_id, date_str)
                
                if showtimes:
                    is_today = (date_str == today_str)
                    date_label = "Auj" if is_today else "Dem"
                    
                    if is_today:
                        print(f"      📋 {cinema_id}: {len(showtimes)} films reçus")
                        if len(showtimes) > 0:
                            print(f"         Exemple: {showtimes[0]}")
                    
                    for show in showtimes:
                        title = show.get('title', 'Film')
                        
                        # Nouveau format: showtimes avec startsAt et diffusionVersion
                        show_times = show.get('showtimes', [])
                        
                        if show_times:
                            # Grouper par version (LOCAL=VF, ORIGINAL=VO)
                            vf_times = []
                            vo_times = []
                            
                            for st in show_times:
                                starts_at = st.get('startsAt', '')
                                version = st.get('diffusionVersion', '')
                                
                                # Extraire l'heure (HH:MM)
                                if 'T' in starts_at:
                                    time_part = starts_at.split('T')[1][:5]
                                else:
                                    time_part = starts_at
                                
                                if version == 'LOCAL':
                                    vf_times.append(f"{time_part}")
                                else:  # ORIGINAL
                                    vo_times.append(f"{time_part}")
                            
                            # Construire la chaîne d'horaires avec date
                            versions = []
                            if vf_times:
                                versions.append(f"VF {date_label}: {', '.join(vf_times[:3])}")
                            if vo_times:
                                versions.append(f"VO {date_label}: {', '.join(vo_times[:3])}")
                            
                            showtimes_str = " | ".join(versions) if versions else ""
                        else:
                            # Ancien format avec VF/VO/VOST
                            vf = show.get('VF', [])
                            vo = show.get('VO', [])
                            vost = show.get('VOST', [])
                            
                            versions = []
                            if vf:
                                versions.append(f"VF {date_label}: {', '.join(vf[:3])}")
                            if vo:
                                versions.append(f"VO {date_label}: {', '.join(vo[:3])}")
                            if vost:
                                versions.append(f"VOST {date_label}: {', '.join(vost[:3])}")
                            
                            showtimes_str = " | ".join(versions) if versions else ""
                        
                        # Ajouter ou fusionner avec film existant
                        if title in all_movies:
                            # Ajouter les horaires
                            if showtimes_str:
                                existing = all_movies[title]['showtimes_str']
                                if existing:
                                    all_movies[title]['showtimes_str'] = existing + " | " + showtimes_str
                                else:
                                    all_movies[title]['showtimes_str'] = showtimes_str
                        else:
                            all_movies[title] = {
                                'title': title,
                                'runtime': 0,
                                'genres': [],
                                'urlPoster': '',
                                'director': '',
                                'isPremiere': False,
                                'weeklyOuting': False,
                                'showtimes_str': showtimes_str,
                                'duration': show.get('duration', ''),
                            }
                elif date_str == today_str:
                    print(f"      📋 {cinema_id}: showtimes vide ou None")
                    
            except Exception as e:
                if date_str == today_str:
                    print(f"      ⚠️ get_showtime({cinema_id}, {date_str}) failed: {e}")
        
        if all_movies:
            return cinema_info, list(all_movies.values())
        
        # Fallback sur get_movies (données enrichies mais moins fiable)
        try:
            movies = api.get_movies(cinema_id, today_str)
            if movies:
                print(f"      📋 {cinema_id}: get_movies retourne {len(movies)} films")
                return cinema_info, movies
        except Exception as e:
            print(f"      ⚠️ get_movies({cinema_id}) failed: {e}")
        
        return cinema_info, []
        
    except Exception as e:
        print(f"      ❌ Erreur cinéma {cinema_info.get('name')}: {e}")
        return cinema_info, []


# ============================================================================
# CINÉMAS ALLOCINÉ - VERSION ULTRA-OPTIMISÉE
# ============================================================================

CINEMAS_ALLOCINE_DATA = []

def load_cinemas_allocine():
    """Charge la base complète des cinémas Allociné avec GPS."""
    global CINEMAS_ALLOCINE_DATA

    def fix_encoding(text):
        """Fix UTF-8 encoding issues (é displayed as Ã©)"""
        if not isinstance(text, str):
            return text
        try:
            # If text contains Ã©, Ã¨, etc., it's UTF-8 bytes interpreted as Latin-1
            # Re-encode as Latin-1 then decode as UTF-8
            return text.encode('latin-1').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            return text

    try:
        allocine_file = os.path.join(os.path.dirname(__file__), 'cinemas_france_data.json')
        if os.path.exists(allocine_file):
            with open(allocine_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Fix encoding issues in cinema names and addresses
            for cinema in data:
                if 'name' in cinema:
                    cinema['name'] = fix_encoding(cinema['name'])
                if 'address' in cinema:
                    cinema['address'] = fix_encoding(cinema['address'])

            CINEMAS_ALLOCINE_DATA = data
            print(f"✅ Cinémas Allociné chargés: {len(CINEMAS_ALLOCINE_DATA)}")
        else:
            print(f"⚠️ Fichier cinemas_france_data.json non trouvé")
    except Exception as e:
        print(f"❌ Erreur chargement cinémas Allociné: {e}")


# Cache des films par cinéma (TTL 1h)
FILMS_CACHE = {}  # {cinema_id: {'films': [...], 'timestamp': datetime}}
FILMS_CACHE_TTL = 3600  # 1 heure


def get_films_cached(cinema, today_str, tomorrow_str=None):
    """Récupère les films avec cache."""
    cinema_id = cinema['id']
    now = time.time()

    # Vérifier le cache
    if cinema_id in FILMS_CACHE:
        cached = FILMS_CACHE[cinema_id]
        if now - cached['timestamp'] < FILMS_CACHE_TTL:
            return cached['films']

    # Pas en cache ou expiré -> requête API
    cinema_info, films = fetch_movies_for_cinema(cinema, today_str, tomorrow_str)

    # Stocker en cache
    FILMS_CACHE[cinema_id] = {
        'films': films,
        'timestamp': now
    }

    return films


def fetch_allocine_cinemas_nearby(center_lat, center_lon, radius_km, max_cinemas=10):
    """
    🚀 VERSION ULTRA-OPTIMISÉE
    
    Utilise directement cinemas_france_data.json (2334 cinémas avec GPS)
    Plus besoin de: CNC matching, département lookup, géocodage
    
    1. Recherche spatiale directe (instantané)
    2. Récupération des films
    """
    
    if not ALLOCINE_AVAILABLE:
        return []
    
    print(f"🎬 Cinéma (optimisé): ({center_lat:.4f}, {center_lon:.4f}), {radius_km}km")
    start_time = time.time()
    
    # Charger la base si pas encore fait
    if not CINEMAS_ALLOCINE_DATA:
        load_cinemas_allocine()
    
    if not CINEMAS_ALLOCINE_DATA:
        print("   ⚠️ Base cinémas non disponible")
        return []
    
    # 1. Recherche spatiale (instantané)
    nearby_cinemas = []
    for cinema in CINEMAS_ALLOCINE_DATA:
        lat = cinema.get('lat')
        lon = cinema.get('lon')
        if not lat or not lon:
            continue
        
        dist = haversine_km(center_lat, center_lon, lat, lon)
        if dist <= radius_km:
            nearby_cinemas.append({
                'id': cinema['id'],
                'name': cinema['name'],
                'address': cinema.get('address', ''),
                'lat': lat,
                'lon': lon,
                'distance': dist
            })
    
    nearby_cinemas.sort(key=lambda c: c['distance'])
    print(f"   📍 {len(nearby_cinemas)} cinémas trouvés")
    
    if not nearby_cinemas:
        return []
    
    # Limiter
    if len(nearby_cinemas) > max_cinemas:
        nearby_cinemas = nearby_cinemas[:max_cinemas]
        print(f"   📍 Limité à {max_cinemas} cinémas")
    
    # 2. Récupérer les films (avec cache)
    today_str = date.today().strftime("%Y-%m-%d")
    tomorrow_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    all_events = []
    cache_hits = 0
    
    print(f"   🎬 Récupération des films...")
    
    for i, cinema in enumerate(nearby_cinemas):
        try:
            cinema_id = cinema['id']
            now = time.time()
            
            # Vérifier le cache
            from_cache = False
            if cinema_id in FILMS_CACHE:
                cached = FILMS_CACHE[cinema_id]
                if now - cached['timestamp'] < FILMS_CACHE_TTL:
                    movies = cached['films']
                    from_cache = True
                    cache_hits += 1
            
            if not from_cache:
                # Requête API (aujourd'hui + demain)
                cinema_info, movies = fetch_movies_for_cinema(cinema, today_str, tomorrow_str)
                # Stocker en cache
                FILMS_CACHE[cinema_id] = {'films': movies, 'timestamp': now}
                # Délai seulement si pas de cache
                if i < len(nearby_cinemas) - 1:
                    time.sleep(0.15)
            else:
                cinema_info = cinema
            
            if movies:
                cache_icon = "💾" if from_cache else "🎬"
                print(f"      {cache_icon} {cinema.get('name', '?')[:30]}: {len(movies)} films")
                for movie in movies:
                    runtime = movie.get('runtime', 0)
                    duration_str = movie.get('duration', '')
                    
                    if runtime and isinstance(runtime, int):
                        h, m = runtime // 3600, (runtime % 3600) // 60
                        duration = f"{h}h{m:02d}" if h else f"{m}min"
                    elif duration_str:
                        duration = duration_str
                    else:
                        duration = ""
                    
                    showtimes_str = movie.get('showtimes_str', '')
                    genres = movie.get('genres', [])
                    genres_str = ", ".join(genres[:3]) if genres else ""
                    
                    desc_parts = []
                    if duration:
                        desc_parts.append(duration)
                    if genres_str:
                        desc_parts.append(genres_str)
                    if movie.get('isPremiere'):
                        desc_parts.append("🌟 Avant-première")
                    if movie.get('weeklyOuting'):
                        desc_parts.append("🆕 Sortie")
                    if showtimes_str:
                        desc_parts.append(showtimes_str)
                    
                    event = {
                        "uid": f"allocine-{cinema_info['id']}-{movie.get('title', '')[:20]}",
                        "title": f"🎬 {movie.get('title', 'Film')}",
                        "begin": today_str,
                        "end": today_str,
                        "locationName": cinema_info['name'],
                        "city": "",
                        "address": cinema_info['address'],
                        "latitude": cinema_info['lat'],
                        "longitude": cinema_info['lon'],
                        "distanceKm": round(cinema_info['distance'], 1),
                        "openagendaUrl": "",
                        "agendaTitle": f"Cinéma {cinema_info['name']}",
                        "source": "Allocine",
                        "description": " • ".join(desc_parts) if desc_parts else "Séances disponibles",
                        "poster": movie.get('urlPoster', ''),
                        "genres": genres,
                    }
                    all_events.append(event)
        except Exception as e:
            print(f"      ❌ Erreur {cinema.get('name', '?')[:20]}: {e}")
    
    print(f"   ✅ {len(all_events)} films en {time.time()-start_time:.1f}s (cache: {cache_hits}/{len(nearby_cinemas)})")
    return all_events


def get_dept_name_from_code(dept_code):
    """Retourne le nom du département depuis son code."""
    DEPT_NAMES = {
        '01': 'ain', '02': 'aisne', '03': 'allier', '04': 'alpes-de-haute-provence',
        '05': 'hautes-alpes', '06': 'alpes-maritimes', '07': 'ardèche', '08': 'ardennes',
        '09': 'ariège', '10': 'aube', '11': 'aude', '12': 'aveyron',
        '13': 'bouches-du-rhône', '14': 'calvados', '15': 'cantal', '16': 'charente',
        '17': 'charente-maritime', '18': 'cher', '19': 'corrèze', '21': 'côte-d\'or',
        '22': 'côtes-d\'armor', '23': 'creuse', '24': 'dordogne', '25': 'doubs',
        '26': 'drôme', '27': 'eure', '28': 'eure-et-loir', '29': 'finistère',
        '30': 'gard', '31': 'haute-garonne', '32': 'gers', '33': 'gironde',
        '34': 'hérault', '35': 'ille-et-vilaine', '36': 'indre', '37': 'indre-et-loire',
        '38': 'isère', '39': 'jura', '40': 'landes', '41': 'loir-et-cher',
        '42': 'loire', '43': 'haute-loire', '44': 'loire-atlantique', '45': 'loiret',
        '46': 'lot', '47': 'lot-et-garonne', '48': 'lozère', '49': 'maine-et-loire',
        '50': 'manche', '51': 'marne', '52': 'haute-marne', '53': 'mayenne',
        '54': 'meurthe-et-moselle', '55': 'meuse', '56': 'morbihan', '57': 'moselle',
        '58': 'nièvre', '59': 'nord', '60': 'oise', '61': 'orne',
        '62': 'pas-de-calais', '63': 'puy-de-dôme', '64': 'pyrénées-atlantiques',
        '65': 'hautes-pyrénées', '66': 'pyrénées-orientales', '67': 'bas-rhin',
        '68': 'haut-rhin', '69': 'rhône', '70': 'haute-saône', '71': 'saône-et-loire',
        '72': 'sarthe', '73': 'savoie', '74': 'haute-savoie', '75': 'paris',
        '76': 'seine-maritime', '77': 'seine-et-marne', '78': 'yvelines',
        '79': 'deux-sèvres', '80': 'somme', '81': 'tarn', '82': 'tarn-et-garonne',
        '83': 'var', '84': 'vaucluse', '85': 'vendée', '86': 'vienne',
        '87': 'haute-vienne', '88': 'vosges', '89': 'yonne', '90': 'territoire-de-belfort',
        '91': 'essonne', '92': 'hauts-de-seine', '93': 'seine-saint-denis',
        '94': 'val-de-marne', '95': 'val-d\'oise', '2A': 'corse-du-sud', '2B': 'haute-corse'
    }
    return DEPT_NAMES.get(str(dept_code), '')


def find_allocine_match(cnc_cinema, allocine_cinemas):
    """
    Trouve la correspondance entre un cinéma CNC et la liste Allociné.
    Utilise les mots-clés et le nom normalisé (sans accents).
    """
    import re
    import unicodedata
    
    def remove_accents(text):
        """Supprime les accents d'un texte."""
        return ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
    
    def extract_keywords(name):
        """Extrait les mots-clés d'un nom (sans accents, sans mots vides)."""
        name_lower = name.lower().strip()
        name_no_accents = remove_accents(name_lower)
        # Remplacer les tirets et caractères spéciaux par des espaces
        name_normalized = re.sub(r'[^a-z0-9]+', ' ', name_no_accents)
        name_normalized = re.sub(r'\s+', ' ', name_normalized).strip()
        
        keywords = set(name_normalized.split())
        # Supprimer les mots vides courts uniquement
        stop_words = {'le', 'la', 'les', 'du', 'de', 'des', 'sur', 'en', 'et'}
        keywords -= stop_words
        return keywords, name_normalized
    
    cnc_keywords, cnc_norm = extract_keywords(cnc_cinema['nom'])
    cnc_commune_norm = remove_accents(cnc_cinema.get('commune', '').lower())
    
    best_match = None
    best_score = 0
    
    for alloc_cinema in allocine_cinemas:
        alloc_name = alloc_cinema.get('name', '')
        alloc_keywords, alloc_norm = extract_keywords(alloc_name)
        
        # Score basé sur les mots-clés communs
        common = cnc_keywords & alloc_keywords
        
        score = len(common) * 10
        
        # Si pas de mots communs, essayer une correspondance partielle
        if not common:
            # Chercher si un mot de l'un contient un mot de l'autre
            for cnc_kw in cnc_keywords:
                for alloc_kw in alloc_keywords:
                    if len(cnc_kw) >= 4 and len(alloc_kw) >= 4:
                        if cnc_kw in alloc_kw or alloc_kw in cnc_kw:
                            score += 8
                            break
        
        if score == 0:
            continue
        
        # Bonus si noms similaires
        if cnc_norm == alloc_norm:
            score += 100
        elif cnc_norm in alloc_norm or alloc_norm in cnc_norm:
            score += 50
        
        # Bonus si commune dans le nom Allociné
        if cnc_commune_norm and len(cnc_commune_norm) > 3:
            alloc_norm_for_commune = remove_accents(alloc_name.lower())
            if cnc_commune_norm in alloc_norm_for_commune:
                score += 30
        
        if score > best_score:
            best_score = score
            best_match = alloc_cinema
    
    return best_match if best_score >= 10 else None



# ============================================================================
# PARALLÉLISATION TOTALE
# ============================================================================

def fetch_all_events_parallel(center_lat, center_lon, radius_km, days_ahead):
    """Exécute DATAtourisme ET OpenAgenda en parallèle."""
    print(f"🔍 Recherche parallèle: ({center_lat}, {center_lon}), {radius_km}km, {days_ahead}j")
    
    all_events = []
    sources_count = {}
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_dt = executor.submit(fetch_datatourisme_events, center_lat, center_lon, radius_km, days_ahead)
        future_oa = executor.submit(fetch_openagenda_events, center_lat, center_lon, radius_km, days_ahead)
        
        try:
            dt_events = future_dt.result(timeout=10)
            sources_count['DATAtourisme'] = len(dt_events)
            all_events.extend(dt_events)
        except Exception as e:
            print(f"   ⚠️ Erreur DATAtourisme: {e}")
            sources_count['DATAtourisme'] = 0
        
        try:
            oa_events = future_oa.result(timeout=25)
            sources_count['OpenAgenda'] = len(oa_events)
            all_events.extend(oa_events)
        except Exception as e:
            print(f"   ⚠️ Erreur OpenAgenda: {e}")
            sources_count['OpenAgenda'] = 0
    
    return all_events, sources_count


# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/events/nearby', methods=['GET'])
def get_nearby_events():
    """Événements à proximité (DATAtourisme + OpenAgenda en parallèle)."""
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        days_ahead = request.args.get('days', DAYS_AHEAD_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({"status": "error", "message": "Paramètres 'lat' et 'lon' requis"}), 400
        
        all_events, sources = fetch_all_events_parallel(center_lat, center_lon, radius_km, days_ahead)
        all_events.sort(key=lambda e: (e.get("distanceKm") or 999, e.get("begin") or ""))
        
        print(f"✅ Total: {len(all_events)} événements")
        
        return jsonify({
            "status": "success",
            "center": {"latitude": center_lat, "longitude": center_lon},
            "radiusKm": radius_km,
            "days": days_ahead,
            "events": all_events,
            "count": len(all_events),
            "sources": sources
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/cinema/nearby', methods=['GET'])
def get_nearby_cinema():
    """Cinémas à proximité (Allociné optimisé) - avec pagination."""
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        batch = request.args.get('batch', 0, type=int)  # 0 = premier batch, 1 = deuxième, etc.
        batch_size = request.args.get('batchSize', 5, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({"status": "error", "message": "Paramètres 'lat' et 'lon' requis"}), 400
        
        # Charger la base si pas encore fait
        if not CINEMAS_ALLOCINE_DATA:
            load_cinemas_allocine()
        
        if not CINEMAS_ALLOCINE_DATA:
            return jsonify({"status": "success", "events": [], "count": 0, "hasMore": False}), 200
        
        # Recherche spatiale (très rapide ~2ms)
        nearby_cinemas = []
        for cinema in CINEMAS_ALLOCINE_DATA:
            lat = cinema.get('lat')
            lon = cinema.get('lon')
            if not lat or not lon:
                continue
            dist = haversine_km(center_lat, center_lon, lat, lon)
            if dist <= radius_km:
                nearby_cinemas.append({
                    'id': cinema['id'],
                    'name': cinema['name'],
                    'address': cinema.get('address', ''),
                    'lat': lat,
                    'lon': lon,
                    'distance': dist
                })
        
        nearby_cinemas.sort(key=lambda c: c['distance'])
        total_cinemas = len(nearby_cinemas)
        
        # Pagination
        start_idx = batch * batch_size
        end_idx = start_idx + batch_size
        cinemas_batch = nearby_cinemas[start_idx:end_idx]
        has_more = end_idx < total_cinemas and end_idx < 50  # Max 50 cinémas total
        
        if not cinemas_batch:
            return jsonify({
                "status": "success",
                "events": [],
                "count": 0,
                "totalCinemas": total_cinemas,
                "batch": batch,
                "hasMore": False
            }), 200
        
        actual_end = min(end_idx, total_cinemas)
        print(f"🎬 Cinéma batch {batch}: cinémas {start_idx+1}-{actual_end} sur {total_cinemas}")
        
        # Récupérer les films pour ce batch
        today_str = date.today().strftime("%Y-%m-%d")
        tomorrow_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        all_events = []
        cache_hits = 0
        start_time = time.time()
        
        for i, cinema in enumerate(cinemas_batch):
            try:
                cinema_id = cinema['id']
                now = time.time()
                
                # Vérifier le cache
                from_cache = False
                if cinema_id in FILMS_CACHE:
                    cached = FILMS_CACHE[cinema_id]
                    if now - cached['timestamp'] < FILMS_CACHE_TTL:
                        movies = cached['films']
                        from_cache = True
                        cache_hits += 1
                
                if not from_cache:
                    cinema_info, movies = fetch_movies_for_cinema(cinema, today_str, tomorrow_str)
                    FILMS_CACHE[cinema_id] = {'films': movies, 'timestamp': now}
                    if i < len(cinemas_batch) - 1:
                        time.sleep(0.5)  # Increased delay to avoid rate limiting
                
                if movies:
                    for movie in movies:
                        runtime = movie.get('runtime', 0)
                        duration_str = movie.get('duration', '')
                        
                        if runtime and isinstance(runtime, int):
                            h, m = runtime // 3600, (runtime % 3600) // 60
                            duration = f"{h}h{m:02d}" if h else f"{m}min"
                        elif duration_str:
                            duration = duration_str
                        else:
                            duration = ""
                        
                        showtimes_str = movie.get('showtimes_str', '')
                        genres = movie.get('genres', [])
                        genres_str = ", ".join(genres[:3]) if genres else ""

                        desc_parts = []
                        if duration:
                            desc_parts.append(duration)
                        if genres_str:
                            desc_parts.append(genres_str)
                        if showtimes_str:
                            desc_parts.append(showtimes_str)

                        # Determine movie date based on showtimes
                        movie_date = today_str  # Default
                        if showtimes_str:
                            # If only "Dem:" and no "Auj:", it's tomorrow
                            if 'Dem:' in showtimes_str and 'Auj:' not in showtimes_str:
                                movie_date = tomorrow_str

                        event = {
                            "uid": f"allocine-{cinema['id']}-{movie.get('title', '')[:20]}",
                            "title": f"🎬 {movie.get('title', 'Film')}",
                            "begin": movie_date,
                            "end": movie_date,
                            "locationName": cinema['name'],
                            "city": "",
                            "address": cinema['address'],
                            "latitude": cinema['lat'],
                            "longitude": cinema['lon'],
                            "distanceKm": round(cinema['distance'], 1),
                            "openagendaUrl": "",
                            "source": "Allocine",
                            "description": " • ".join(desc_parts) if desc_parts else "",
                        }
                        all_events.append(event)
            except Exception as e:
                print(f"      ❌ Erreur {cinema.get('name', '?')[:20]}: {e}")
        
        elapsed = time.time() - start_time
        print(f"   ✅ Batch {batch}: {len(all_events)} films en {elapsed:.1f}s (cache: {cache_hits}/{len(cinemas_batch)})")
        
        return jsonify({
            "status": "success",
            "center": {"latitude": center_lat, "longitude": center_lon},
            "radiusKm": radius_km,
            "events": all_events,
            "count": len(all_events),
            "totalCinemas": total_cinemas,
            "batch": batch,
            "hasMore": has_more,
            "source": "Allocine"
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# SALONS (EventsEye)
# ============================================================================

SALONS_DATA = []

def load_salons_data():
    """Charge les données des salons depuis le fichier JSON."""
    global SALONS_DATA
    try:
        import os
        salons_file = os.path.join(os.path.dirname(__file__), 'salons_france.json')
        if os.path.exists(salons_file):
            with open(salons_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Gérer les deux formats possibles
            if isinstance(data, list):
                # Format attendu: liste de salons
                SALONS_DATA = data
            elif isinstance(data, dict) and 'events' in data:
                # Format eventseye: {"events": [...]}
                SALONS_DATA = data['events']
            else:
                print(f"⚠️ Format de fichier salons inconnu: {type(data)}")
                SALONS_DATA = []
            
            # Vérifier que les éléments sont des dicts
            if SALONS_DATA and not isinstance(SALONS_DATA[0], dict):
                print(f"⚠️ Format invalide: les salons ne sont pas des dictionnaires")
                print(f"   Type premier élément: {type(SALONS_DATA[0])}")
                print(f"   Contenu: {str(SALONS_DATA[0])[:100]}")
                SALONS_DATA = []
            else:
                print(f"✅ Salons chargés: {len(SALONS_DATA)}")
        else:
            print(f"⚠️ Fichier salons_france.json non trouvé")
    except Exception as e:
        print(f"❌ Erreur chargement salons: {e}")
        import traceback
        traceback.print_exc()


def parse_salon_date(date_str):
    """Parse une date de salon (formats: DD/MM/YYYY ou 'mois YYYY')."""
    if not date_str:
        return None
    
    try:
        from datetime import datetime
        
        # Format 1: DD/MM/YYYY
        if '/' in date_str:
            return datetime.strptime(date_str, '%d/%m/%Y').date()
        
        # Format 2: "mois YYYY" (janv. 2026, avril 2026, etc.)
        MOIS = {
            'janv': 1, 'janvier': 1,
            'fév': 2, 'fevr': 2, 'février': 2, 'fevrier': 2,
            'mars': 3,
            'avril': 4, 'avr': 4,
            'mai': 5,
            'juin': 6,
            'juil': 7, 'juillet': 7,
            'août': 8, 'aout': 8,
            'sept': 9, 'septembre': 9,
            'oct': 10, 'octobre': 10,
            'nov': 11, 'novembre': 11,
            'déc': 12, 'dec': 12, 'décembre': 12, 'decembre': 12
        }
        
        date_lower = date_str.lower().replace('.', '').strip()
        
        for mois_str, mois_num in MOIS.items():
            if mois_str in date_lower:
                # Extraire l'année
                import re
                year_match = re.search(r'(\d{4})', date_str)
                if year_match:
                    year = int(year_match.group(1))
                    # Retourner le 1er du mois
                    return datetime(year, mois_num, 1).date()
        
        return None
    except:
        return None


@app.route('/api/salons/nearby', methods=['GET'])
def get_nearby_salons():
    """Salons à proximité - tous les salons à venir (2025-2026)."""
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({"status": "error", "message": "Paramètres 'lat' et 'lon' requis"}), 400
        
        # Charger les salons si pas encore fait
        if not SALONS_DATA:
            load_salons_data()
        
        print(f"🏢 Recherche salons: ({center_lat}, {center_lon}), rayon={radius_km}km")
        print(f"   Total salons en mémoire: {len(SALONS_DATA)}")
        
        today = date.today()
        nearby_salons = []
        
        for salon in SALONS_DATA:
            lat = salon.get('lat')
            lon = salon.get('lon')
            
            if not lat or not lon:
                continue
            
            # Filtrer par distance
            dist = haversine_km(center_lat, center_lon, lat, lon)
            if dist > radius_km:
                continue
            
            # Filtrer les salons passés
            salon_date = parse_salon_date(salon.get('dates', ''))
            if salon_date and salon_date < today:
                continue
            
            nearby_salons.append({
                "uid": f"salon-{hash(salon['name']) % 100000}",
                "title": salon['name'],
                "begin": salon.get('dates', ''),
                "duration": salon.get('duration', ''),
                "locationName": salon.get('venue', ''),
                "city": salon.get('city', ''),
                "latitude": lat,
                "longitude": lon,
                "distanceKm": round(dist, 1),
                "frequency": salon.get('frequency', ''),
                "openagendaUrl": salon.get('url', ''),
                "source": "EventsEye"
            })
        
        # Trier par distance
        nearby_salons.sort(key=lambda s: s['distanceKm'])
        
        print(f"🏢 Salons: {len(nearby_salons)} trouvés dans {radius_km}km")
        
        return jsonify({
            "status": "success",
            "center": {"latitude": center_lat, "longitude": center_lon},
            "radiusKm": radius_km,
            "events": nearby_salons,
            "count": len(nearby_salons),
            "source": "EventsEye"
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur salons: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/debug/allocine-depts', methods=['GET'])
def debug_allocine_depts():
    """
    🔧 DEBUG: Récupère la liste complète des départements Allociné avec leurs vrais IDs.
    Utilise cet endpoint pour corriger le mapping dans department_mapping.py
    """
    if not ALLOCINE_AVAILABLE:
        return jsonify({"status": "error", "message": "Allociné API non disponible"}), 500
    
    try:
        api = allocineAPI()
        depts = api.get_departements()
        
        # Trier par nom pour faciliter la lecture
        depts_sorted = sorted(depts, key=lambda d: d.get('name', ''))
        
        # Créer un mapping prêt à copier-coller
        mapping_code = []
        postcode_mapping = []
        
        for dept in depts_sorted:
            name = dept.get('name', '')
            dept_id = dept.get('id', '')
            
            # Extraire le numéro de département du nom si possible
            name_lower = name.lower().strip()
            mapping_code.append(f'    "{name_lower}": "{dept_id}",')
        
        return jsonify({
            "status": "success",
            "count": len(depts),
            "departments": depts_sorted,
            "mapping_code": "\n".join(mapping_code)
        }), 200
        
    except Exception as e:
        import traceback
        return jsonify({
            "status": "error", 
            "message": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Statistiques de la base."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) as total FROM evenements")
        total = cur.fetchone()['total']
        
        cur.execute("SELECT COUNT(*) as count FROM evenements WHERE date_debut >= CURRENT_DATE")
        futurs = cur.fetchone()['count']
        
        cur.execute("""
            SELECT commune, COUNT(*) as count FROM evenements
            WHERE commune IS NOT NULL GROUP BY commune ORDER BY count DESC LIMIT 10
        """)
        top_communes = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "total_events": total,
            "upcoming_events": futurs,
            "top_communes": [dict(row) for row in top_communes],
            "sources": ["DATAtourisme", "OpenAgenda", "Allociné (optimisé)"]
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "sources": ["DATAtourisme", "OpenAgenda", "Allociné" if ALLOCINE_AVAILABLE else "Allociné (non dispo)"],
            "optimizations": ["mapping statique", "recherche IDF élargie", "cache cinémas"]
        }), 200
        
    except Exception as e:
        return jsonify({"status": "unhealthy", "database": "disconnected", "error": str(e)}), 500


# ============================================================================
# 📷 PROXY GEMINI (pour éviter les problèmes CORS)
# ============================================================================

# IMPORTANT: Ne jamais mettre la clé dans le code !
# Configurer GEMINI_API_KEY dans les variables d'environnement Render
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-2.5-pro"]

@app.route('/api/scanner/analyze', methods=['POST'])
def analyze_poster():
    """
    Proxy pour l'API Gemini - Analyse d'affiches d'événements
    Évite les problèmes CORS en faisant la requête côté serveur
    """
    try:
        # Vérifier que la clé API est configurée
        if not GEMINI_API_KEY:
            return jsonify({
                "status": "error", 
                "message": "GEMINI_API_KEY non configurée. Ajoutez-la dans les variables d'environnement Render."
            }), 500
        
        data = request.get_json()
        
        if not data:
            return jsonify({"status": "error", "message": "Données manquantes"}), 400
        
        base64_image = data.get('image')
        mime_type = data.get('mimeType', 'image/jpeg')
        
        if not base64_image:
            return jsonify({"status": "error", "message": "Image manquante"}), 400
        
        prompt = """Analyse cette image d'affiche événementielle ou publicitaire.
Extrais toutes les informations et retourne UNIQUEMENT un JSON valide avec cette structure exacte:
{
    "title": "Titre de l'événement",
    "category": "Concert|Théâtre|Atelier|Conférence|Sport|Exposition|Festival|Autre",
    "startDate": "YYYY-MM-DD ou texte brut",
    "startTime": "HH:MM ou null",
    "endDate": "YYYY-MM-DD ou null",
    "endTime": "HH:MM ou null",
    "location": {
        "venueName": "Nom du lieu",
        "address": "Adresse ou null",
        "city": "Ville"
    },
    "organizer": {
        "name": "Nom ou null"
    },
    "website": "URL du site web de l'événement ou null",
    "pricing": {
        "isFree": true/false,
        "priceRange": "10€ - 25€ ou null",
        "currency": "EUR ou null"
    },
    "description": "Résumé court (max 3 phrases) ou null",
    "tags": ["tag1", "tag2"]
}

Réponds UNIQUEMENT avec le JSON, sans markdown ni explications."""

        # Essayer plusieurs modèles
        last_error = None
        
        for model in GEMINI_MODELS:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
                
                request_body = {
                    "contents": [{
                        "parts": [
                            {
                                "inlineData": {
                                    "mimeType": mime_type,
                                    "data": base64_image
                                }
                            },
                            {"text": prompt}
                        ]
                    }],
                    "generationConfig": {
                        "temperature": 0.2,
                        "maxOutputTokens": 2048
                    }
                }
                
                print(f"🤖 Gemini: Tentative avec {model}...")
                
                response = requests.post(url, json=request_body, timeout=60)
                
                if response.status_code == 200:
                    result = response.json()
                    text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
                    
                    if text:
                        # Nettoyer le JSON
                        json_text = text.strip()
                        if json_text.startswith('```'):
                            json_text = json_text.replace('```json', '').replace('```', '').strip()
                        
                        import json
                        event_data = json.loads(json_text)
                        print(f"✅ Gemini: Analyse réussie avec {model}")
                        
                        return jsonify({
                            "status": "success",
                            "data": event_data,
                            "model": model
                        }), 200
                else:
                    last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                    print(f"⚠️ Gemini {model}: {last_error}")
                    
            except Exception as e:
                last_error = str(e)
                print(f"⚠️ Gemini {model} erreur: {e}")
                continue
        
        return jsonify({
            "status": "error",
            "message": f"Tous les modèles ont échoué. Dernière erreur: {last_error}"
        }), 500
        
    except Exception as e:
        print(f"❌ Erreur proxy Gemini: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/scanner/geocode', methods=['POST'])
def geocode_address():
    """
    Géocode une adresse via Nominatim
    """
    try:
        data = request.get_json()
        address = data.get('address', '')
        
        if not address:
            return jsonify({"status": "error", "message": "Adresse manquante"}), 400
        
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json", "limit": 1}
        headers = {"User-Agent": "GEDEON-Scanner/1.0 (contact@gedeon.app)"}
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            results = response.json()
            if results:
                return jsonify({
                    "status": "success",
                    "latitude": float(results[0]['lat']),
                    "longitude": float(results[0]['lon']),
                    "displayName": results[0].get('display_name', '')
                }), 200
            else:
                return jsonify({"status": "error", "message": "Adresse non trouvée"}), 404
        else:
            return jsonify({"status": "error", "message": f"Erreur Nominatim: {response.status_code}"}), 500
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# 👤 API USERS
# ============================================================================

import re

def validate_pseudo(pseudo):
    """Valide un pseudo (2-20 chars, lettres/chiffres/underscore)"""
    if not pseudo or len(pseudo) < 2 or len(pseudo) > 20:
        return False, "Le pseudo doit faire entre 2 et 20 caractères"
    if not re.match(r'^[a-zA-Z0-9_àâäéèêëïîôùûüç]+$', pseudo):
        return False, "Le pseudo ne peut contenir que des lettres, chiffres et underscore"
    return True, None


@app.route('/api/init', methods=['GET', 'POST'])
def api_init():
    """
    Endpoint pour initialiser les tables manuellement.
    Utile pour le premier déploiement.
    """
    global USER_TABLES_INITIALIZED
    
    try:
        if init_user_tables():
            USER_TABLES_INITIALIZED = True
            return jsonify({
                "status": "success",
                "message": "Tables users et scanned_events créées"
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Erreur lors de la création des tables"
            }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/api/users/register', methods=['POST'])
def user_register():
    """
    Inscription d'un nouvel utilisateur.
    Requiert: email + pseudo + password
    Envoie un email de confirmation.
    """
    global USER_TABLES_INITIALIZED
    
    try:
        # Initialiser les tables si pas encore fait
        if not USER_TABLES_INITIALIZED:
            if init_user_tables():
                USER_TABLES_INITIALIZED = True
        
        data = request.get_json()
        email = data.get('email', '').strip().lower()
        pseudo = data.get('pseudo', '').strip()
        password = data.get('password', '')
        device_id = data.get('device_id', '').strip()
        
        # Validation email
        if not email or not is_valid_email(email):
            return jsonify({"status": "error", "message": "Email invalide"}), 400
        
        # Validation pseudo
        valid, error = validate_pseudo(pseudo)
        if not valid:
            return jsonify({"status": "error", "message": error}), 400
        
        # Validation mot de passe
        if not password or len(password) < 4:
            return jsonify({"status": "error", "message": "Mot de passe: 4 caractères minimum"}), 400
        
        if len(password) > 50:
            return jsonify({"status": "error", "message": "Mot de passe trop long (50 max)"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Vérifier si l'email existe déjà
        cur.execute("SELECT id, email_confirmed FROM users WHERE email = %s", (email,))
        existing_email = cur.fetchone()
        if existing_email:
            cur.close()
            conn.close()
            if not existing_email['email_confirmed']:
                return jsonify({"status": "error", "message": "Cet email est en attente de confirmation. Vérifiez vos emails."}), 409
            return jsonify({"status": "error", "message": "Cet email est déjà utilisé"}), 409
        
        # Vérifier si le pseudo existe déjà
        cur.execute("SELECT id FROM users WHERE pseudo = %s", (pseudo,))
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Ce pseudo est déjà pris"}), 409
        
        # Générer le token de confirmation
        confirmation_token = generate_confirmation_token()
        
        # Créer l'utilisateur (non confirmé)
        password_hash = generate_password_hash(password)
        cur.execute(
            """INSERT INTO users (email, pseudo, password_hash, device_id, email_confirmed, confirmation_token, confirmation_sent_at) 
               VALUES (%s, %s, %s, %s, FALSE, %s, CURRENT_TIMESTAMP) 
               RETURNING id, pseudo, email""",
            (email, pseudo, password_hash, device_id or None, confirmation_token)
        )
        new_user = cur.fetchone()
        conn.commit()
        
        # Envoyer l'email de confirmation
        if AUTH_EMAIL_AVAILABLE:
            success, error_msg = send_confirmation_email(email, pseudo, confirmation_token)
            if not success:
                print(f"⚠️ Erreur envoi email: {error_msg}")
        
        cur.close()
        conn.close()
        
        print(f"👤 Inscription: {pseudo} ({email}) - En attente de confirmation")
        
        return jsonify({
            "status": "success",
            "message": "Compte créé ! Vérifiez votre email pour confirmer.",
            "user": {
                "id": new_user['id'],
                "pseudo": new_user['pseudo'],
                "email": new_user['email'],
                "confirmed": False
            }
        }), 201
        
    except Exception as e:
        print(f"❌ Erreur register: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/users/resend-confirmation', methods=['POST'])
def resend_confirmation():
    """Renvoie l'email de confirmation"""
    try:
        data = request.get_json()
        email = data.get('email', '').strip().lower()
        
        if not email:
            return jsonify({"status": "error", "message": "Email requis"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT id, pseudo, email_confirmed, confirmation_sent_at FROM users WHERE email = %s",
            (email,)
        )
        user = cur.fetchone()
        
        if not user:
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Email non trouvé"}), 404
        
        if user['email_confirmed']:
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Email déjà confirmé"}), 400
        
        # Générer nouveau token
        new_token = generate_confirmation_token()
        cur.execute(
            "UPDATE users SET confirmation_token = %s, confirmation_sent_at = CURRENT_TIMESTAMP WHERE id = %s",
            (new_token, user['id'])
        )
        conn.commit()
        
        # Envoyer l'email
        if AUTH_EMAIL_AVAILABLE:
            send_confirmation_email(email, user['pseudo'], new_token)
        
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "message": "Email de confirmation renvoyé"}), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/confirm', methods=['GET'])
def confirm_email():
    """
    Confirme l'email via le lien cliqué.
    Redirige vers la page de succès.
    """
    token = request.args.get('token', '').strip()
    
    if not token:
        return redirect('/confirmation-error.html?error=missing_token')
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT id, pseudo, email_confirmed, confirmation_sent_at FROM users WHERE confirmation_token = %s",
            (token,)
        )
        user = cur.fetchone()
        
        if not user:
            cur.close()
            conn.close()
            return redirect('/confirmation-error.html?error=invalid_token')
        
        if user['email_confirmed']:
            cur.close()
            conn.close()
            return redirect('/confirmation-success.html?already=true')
        
        # Vérifier expiration (24h)
        if is_token_expired(user['confirmation_sent_at']):
            cur.close()
            conn.close()
            return redirect('/confirmation-error.html?error=expired_token')
        
        # Confirmer l'email
        cur.execute(
            "UPDATE users SET email_confirmed = TRUE, confirmation_token = NULL WHERE id = %s",
            (user['id'],)
        )
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"✅ Email confirmé: {user['pseudo']}")
        
        return redirect('/confirmation-success.html')
        
    except Exception as e:
        print(f"❌ Erreur confirmation: {e}")
        return redirect('/confirmation-error.html?error=server_error')


@app.route('/api/users/login', methods=['POST'])
def user_login():
    """
    Connexion d'un utilisateur existant.
    Requiert: email + password
    L'email doit être confirmé.
    """
    global USER_TABLES_INITIALIZED
    
    try:
        # Initialiser les tables si pas encore fait
        if not USER_TABLES_INITIALIZED:
            if init_user_tables():
                USER_TABLES_INITIALIZED = True
        
        data = request.get_json()
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        device_id = data.get('device_id', '').strip()
        
        # Validation
        if not email:
            return jsonify({"status": "error", "message": "Email requis"}), 400
        
        if not password:
            return jsonify({"status": "error", "message": "Mot de passe requis"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Chercher l'utilisateur par email
        cur.execute(
            "SELECT id, pseudo, email, password_hash, email_confirmed, device_id FROM users WHERE email = %s",
            (email,)
        )
        user = cur.fetchone()
        
        if not user:
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Email ou mot de passe incorrect"}), 401
        
        # Vérifier le mot de passe
        if not user['password_hash'] or not check_password_hash(user['password_hash'], password):
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Email ou mot de passe incorrect"}), 401
        
        # Vérifier si l'email est confirmé
        if not user['email_confirmed']:
            cur.close()
            conn.close()
            return jsonify({
                "status": "error", 
                "message": "Email non confirmé. Vérifiez votre boîte mail.",
                "code": "EMAIL_NOT_CONFIRMED"
            }), 403
        
        # Mise à jour last_seen et device_id
        cur.execute(
            "UPDATE users SET last_seen = CURRENT_TIMESTAMP, device_id = %s WHERE id = %s",
            (device_id or user['device_id'], user['id'])
        )
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"👤 Connexion: {user['pseudo']} ({email})")
        
        return jsonify({
            "status": "success",
            "user": {
                "id": user['id'],
                "pseudo": user['pseudo'],
                "email": user['email'],
                "confirmed": True
            }
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur login: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/users/verify', methods=['POST'])
def verify_user():
    """
    Vérifie qu'un utilisateur est valide et confirmé.
    Appelé avant les actions sensibles (ajout de scan, etc.)
    """
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        email = data.get('email', '').strip().lower()
        
        if not user_id or not email:
            return jsonify({"status": "error", "message": "user_id et email requis"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT id, pseudo, email, email_confirmed FROM users WHERE id = %s AND email = %s",
            (user_id, email)
        )
        user = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not user:
            return jsonify({"status": "error", "message": "Utilisateur non trouvé"}), 401
        
        if not user['email_confirmed']:
            return jsonify({
                "status": "error", 
                "message": "Email non confirmé",
                "code": "EMAIL_NOT_CONFIRMED"
            }), 403
        
        return jsonify({
            "status": "success",
            "user": {
                "id": user['id'],
                "pseudo": user['pseudo'],
                "email": user['email'],
                "confirmed": True
            }
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur verify: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/users/forgot-password', methods=['POST'])
def forgot_password():
    """Demande de réinitialisation du mot de passe"""
    try:
        data = request.get_json()
        email = data.get('email', '').strip().lower()
        
        if not email:
            return jsonify({"status": "error", "message": "Email requis"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT id, pseudo, email_confirmed FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        
        # Ne pas révéler si l'email existe ou pas
        if user and user['email_confirmed']:
            reset_token = generate_confirmation_token()
            cur.execute(
                "UPDATE users SET reset_token = %s, reset_sent_at = CURRENT_TIMESTAMP WHERE id = %s",
                (reset_token, user['id'])
            )
            conn.commit()
            
            if AUTH_EMAIL_AVAILABLE:
                send_password_reset_email(email, user['pseudo'], reset_token)
        
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "success", 
            "message": "Si cet email existe, vous recevrez un lien de réinitialisation."
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/users/reset-password', methods=['POST'])
def reset_password():
    """Réinitialise le mot de passe avec le token"""
    try:
        data = request.get_json()
        token = data.get('token', '').strip()
        new_password = data.get('password', '')
        
        if not token:
            return jsonify({"status": "error", "message": "Token requis"}), 400
        
        if not new_password or len(new_password) < 4:
            return jsonify({"status": "error", "message": "Mot de passe: 4 caractères minimum"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT id, pseudo, reset_sent_at FROM users WHERE reset_token = %s",
            (token,)
        )
        user = cur.fetchone()
        
        if not user:
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Token invalide"}), 400
        
        # Vérifier expiration (1h pour reset)
        if is_token_expired(user['reset_sent_at'], hours=1):
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Token expiré. Refaites une demande."}), 400
        
        # Mettre à jour le mot de passe
        password_hash = generate_password_hash(new_password)
        cur.execute(
            "UPDATE users SET password_hash = %s, reset_token = NULL WHERE id = %s",
            (password_hash, user['id'])
        )
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"🔐 Mot de passe réinitialisé: {user['pseudo']}")
        
        return jsonify({"status": "success", "message": "Mot de passe modifié !"}), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/users', methods=['GET'])
def list_users():
    """Liste tous les utilisateurs"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT u.id, u.pseudo, u.created_at, u.last_seen,
                   COUNT(s.id) as scan_count
            FROM users u
            LEFT JOIN scanned_events s ON u.id = s.user_id
            GROUP BY u.id
            ORDER BY u.pseudo
        """)
        users = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "users": [dict(u) for u in users]
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# 📷 API SCANNED EVENTS
# ============================================================================

@app.route('/api/scanned', methods=['GET'])
def get_scanned_events():
    """
    Récupère les événements scannés.
    
    Params:
    - user_id: (optionnel) Filtrer par utilisateur
    - mine: (optionnel) Si "1" et user_id fourni, inclut les événements privés de cet utilisateur
    """
    try:
        user_id = request.args.get('user_id', type=int)
        mine_only = request.args.get('mine', '') == '1'
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        if user_id and mine_only:
            # L'utilisateur veut voir TOUS les publics + SES privés
            cur.execute("""
                WITH numbered_scans AS (
                    SELECT s.*, u.pseudo as user_pseudo,
                           ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.created_at ASC) as scan_number,
                           COUNT(*) OVER (PARTITION BY s.user_id) as total_scans
                    FROM scanned_events s
                    JOIN users u ON s.user_id = u.id
                )
                SELECT * FROM numbered_scans
                WHERE is_private = FALSE
                   OR (is_private = TRUE AND user_id = %s)
                ORDER BY created_at DESC
            """, (user_id,))
        elif user_id:
            # Voir les événements d'un utilisateur spécifique (publics uniquement)
            cur.execute("""
                WITH numbered_scans AS (
                    SELECT s.*, u.pseudo as user_pseudo,
                           ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.created_at ASC) as scan_number,
                           COUNT(*) OVER (PARTITION BY s.user_id) as total_scans
                    FROM scanned_events s
                    JOIN users u ON s.user_id = u.id
                )
                SELECT * FROM numbered_scans
                WHERE user_id = %s AND is_private = FALSE
                ORDER BY created_at DESC
            """, (user_id,))
        else:
            # Tous les événements publics
            cur.execute("""
                WITH numbered_scans AS (
                    SELECT s.*, u.pseudo as user_pseudo,
                           ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.created_at ASC) as scan_number,
                           COUNT(*) OVER (PARTITION BY s.user_id) as total_scans
                    FROM scanned_events s
                    JOIN users u ON s.user_id = u.id
                )
                SELECT * FROM numbered_scans
                WHERE is_private = FALSE
                ORDER BY created_at DESC
            """)
        
        events = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Formater les événements
        formatted = []
        for e in events:
            event = dict(e)
            # Convertir les dates en string
            if event.get('begin_date'):
                event['begin_date'] = event['begin_date'].isoformat()
            if event.get('end_date'):
                event['end_date'] = event['end_date'].isoformat()
            if event.get('created_at'):
                event['created_at'] = event['created_at'].isoformat()
            formatted.append(event)
        
        return jsonify({
            "status": "success",
            "events": formatted,
            "count": len(formatted)
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur get scanned: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/scanned', methods=['POST'])
def add_scanned_event():
    """
    Ajoute un événement scanné.
    
    Body JSON requis:
    - user_id: ID de l'utilisateur
    - title: Titre de l'événement
    - is_private: (optionnel) true/false
    - ... autres champs
    """
    try:
        import hashlib
        import json
        from difflib import SequenceMatcher
        
        data = request.get_json()
        
        user_id = data.get('user_id')
        if not user_id:
            return jsonify({"status": "error", "message": "user_id requis"}), 400

        # Validation de la localisation (latitude/longitude requis)
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        if latitude is None or longitude is None:
            return jsonify({"status": "error", "message": "Localisation requise (latitude et longitude)"}), 400

        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "Latitude et longitude doivent être des nombres"}), 400

        # Fonction de similarité de titre
        def normalize_title(title):
            if not title:
                return ""
            t = title.lower().strip()
            for char in ".,;:!?-_'\"()[]{}":
                t = t.replace(char, " ")
            return " ".join(t.split())
        
        def title_similarity(a, b):
            if not a or not b:
                return 0
            return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100
        
        # Fonction de distance GPS
        def haversine_km(lat1, lon1, lat2, lon2):
            if None in (lat1, lon1, lat2, lon2):
                return None
            import math
            R = 6371.0
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlambda = math.radians(lon2 - lon1)
            a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        # Générer un UID unique basé sur le hash du contenu
        content_for_hash = {
            "title": data.get('title'),
            "category": data.get('category'),
            "begin": data.get('begin'),
            "end": data.get('end'),
            "startTime": data.get('startTime'),
            "endTime": data.get('endTime'),
            "locationName": data.get('locationName'),
            "city": data.get('city'),
            "address": data.get('address'),
            "description": data.get('description'),
            "organizer": data.get('organizer')
        }
        content_json = json.dumps(content_for_hash, sort_keys=True, ensure_ascii=False)
        content_hash = hashlib.sha256(content_json.encode('utf-8')).hexdigest()[:16]
        uid = f"scanned-{content_hash}"
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Vérifier que l'utilisateur existe
        cur.execute("SELECT id FROM users WHERE id = %s", (user_id,))
        if not cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Utilisateur non trouvé"}), 404
        
        # Vérifier si cet événement existe déjà (hash exact)
        cur.execute("SELECT id FROM scanned_events WHERE uid = %s", (uid,))
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Cet événement a déjà été scanné (identique)"}), 409
        
        # 🔍 DÉTECTION INTELLIGENTE DE DOUBLONS
        # Chercher des événements similaires (même titre + même ville/GPS)
        new_title = data.get('title', '')
        new_city = data.get('city', '')
        new_lat = data.get('latitude')
        new_lon = data.get('longitude')
        
        cur.execute("""
            SELECT id, title, city, latitude, longitude 
            FROM scanned_events
        """)
        existing_events = cur.fetchall()
        
        for existing in existing_events:
            # Comparer les titres
            title_match = normalize_title(new_title) == normalize_title(existing['title'])
            title_sim = title_similarity(new_title, existing['title'])
            
            if not title_match and title_sim < 80:
                continue  # Titres trop différents
            
            # Comparer la localisation
            same_city = (new_city or '').lower() == (existing['city'] or '').lower() if new_city and existing['city'] else False
            distance = haversine_km(new_lat, new_lon, existing['latitude'], existing['longitude'])
            close_location = distance is not None and distance < 1  # < 1km
            
            if same_city or close_location:
                cur.close()
                conn.close()
                return jsonify({
                    "status": "error", 
                    "message": f"Un événement similaire existe déjà : '{existing['title']}' (ID={existing['id']})"
                }), 409
        
        # Parser les dates
        begin_date = None
        end_date = None
        if data.get('begin'):
            try:
                begin_date = datetime.strptime(data['begin'][:10], '%Y-%m-%d').date()
            except:
                pass
        if data.get('end'):
            try:
                end_date = datetime.strptime(data['end'][:10], '%Y-%m-%d').date()
            except:
                pass

        # Gérer l'image si fournie
        image_path = None
        if data.get('image'):
            try:
                import base64
                import os

                # Extraire les données base64
                image_data = data['image']
                if ',' in image_data:
                    # Format: "data:image/jpeg;base64,/9j/4AAQ..."
                    image_data = image_data.split(',', 1)[1]

                # Décoder le base64
                image_bytes = base64.b64decode(image_data)

                # Créer le nom de fichier avec l'uid
                uploads_dir = os.path.join(UPLOADS_BASE_DIR, 'scans')

                # Debug: show actual path
                print(f"🔍 DEBUG: uploads_dir = {uploads_dir}")
                print(f"🔍 DEBUG: __file__ = {__file__}")

                os.makedirs(uploads_dir, exist_ok=True)

                # Déterminer l'extension (jpeg par défaut)
                extension = 'jpg'
                if data['image'].startswith('data:image/png'):
                    extension = 'png'
                elif data['image'].startswith('data:image/webp'):
                    extension = 'webp'

                filename = f"{uid}.{extension}"
                filepath = os.path.join(uploads_dir, filename)

                print(f"🔍 DEBUG: filepath = {filepath}")

                # Sauvegarder l'image
                with open(filepath, 'wb') as f:
                    f.write(image_bytes)

                # Verify file was written
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    print(f"✅ File written: {filepath} ({file_size} bytes)")
                else:
                    print(f"❌ File NOT found after write: {filepath}")

                # Stocker le chemin relatif
                image_path = f"uploads/scans/{filename}"

                print(f"💾 Image sauvegardée: {image_path}")

            except Exception as e:
                print(f"⚠️  Erreur sauvegarde image: {e}")
                # Continue sans l'image si erreur

        # Insérer l'événement
        cur.execute("""
            INSERT INTO scanned_events (
                user_id, uid, title, category, begin_date, end_date,
                start_time, end_time, location_name, city, address,
                latitude, longitude, description, organizer, pricing,
                website, tags, is_private, image_path
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id, uid, created_at
        """, (
            user_id,
            uid,
            data.get('title'),
            data.get('category'),
            begin_date,
            end_date,
            data.get('startTime'),
            data.get('endTime'),
            data.get('locationName'),
            data.get('city'),
            data.get('address'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('description'),
            data.get('organizer'),
            data.get('pricing'),
            data.get('website'),
            data.get('tags', []),
            data.get('is_private', False),
            image_path
        ))
        
        result = cur.fetchone()
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"📷 Événement scanné ajouté: {data.get('title')} par user {user_id}")
        
        return jsonify({
            "status": "success",
            "event": {
                "id": result['id'],
                "uid": result['uid'],
                "created_at": result['created_at'].isoformat()
            }
        }), 201
        
    except Exception as e:
        print(f"❌ Erreur add scanned: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/scanned/<int:event_id>', methods=['DELETE'])
def delete_scanned_event(event_id):
    """
    Supprime un événement scanné.
    Seul le propriétaire peut supprimer.
    
    Params:
    - user_id: ID de l'utilisateur (pour vérification)
    """
    try:
        user_id = request.args.get('user_id', type=int)
        
        if not user_id:
            return jsonify({"status": "error", "message": "user_id requis"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Vérifier que l'événement appartient à l'utilisateur
        cur.execute(
            "SELECT id, user_id, title FROM scanned_events WHERE id = %s",
            (event_id,)
        )
        event = cur.fetchone()
        
        if not event:
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Événement non trouvé"}), 404
        
        if event['user_id'] != user_id:
            cur.close()
            conn.close()
            return jsonify({"status": "error", "message": "Non autorisé"}), 403
        
        # Supprimer
        cur.execute("DELETE FROM scanned_events WHERE id = %s", (event_id,))
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"🗑️ Événement scanné supprimé: {event['title']}")
        
        return jsonify({"status": "success", "message": "Événement supprimé"}), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/scanned/reset', methods=['DELETE'])
def reset_scanned_events():
    """
    Supprime TOUS les événements scannés d'un utilisateur.
    Permet de repartir à zéro.
    
    Params:
    - user_id: ID de l'utilisateur
    """
    try:
        user_id = request.args.get('user_id', type=int)
        
        if not user_id:
            return jsonify({"status": "error", "message": "user_id requis"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Compter avant suppression
        cur.execute("SELECT COUNT(*) as count FROM scanned_events WHERE user_id = %s", (user_id,))
        count = cur.fetchone()['count']
        
        # Supprimer tous les scans de cet utilisateur
        cur.execute("DELETE FROM scanned_events WHERE user_id = %s", (user_id,))
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"🗑️ Reset: {count} événements supprimés pour user_id={user_id}")
        
        return jsonify({
            "status": "success", 
            "message": f"{count} événement(s) supprimé(s)",
            "deleted_count": count
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# STATIC FILE SERVING - UPLOADS
# ============================================================================

@app.route('/api/test-route')
def test_route():
    """Test route to verify routing works"""
    return jsonify({"status": "ok", "message": "Route works!"})

@app.route('/api/files/<path:filename>')
def serve_upload(filename):
    """Sert les fichiers uploadés (images de scans)"""
    import sys
    uploads_dir = UPLOADS_BASE_DIR
    filepath = os.path.join(uploads_dir, filename)

    print(f"🔍 ROUTE DEBUG: Requested /api/files/{filename}")
    print(f"🔍 ROUTE DEBUG: uploads_dir = {uploads_dir}")
    print(f"🔍 ROUTE DEBUG: filepath = {filepath}")
    print(f"🔍 ROUTE DEBUG: File exists? {os.path.exists(filepath)}")
    sys.stdout.flush()

    if os.path.exists(filepath):
        print(f"✅ ROUTE: Serving file {filepath}")
    else:
        print(f"❌ ROUTE: File not found {filepath}")

    sys.stdout.flush()
    return send_from_directory(uploads_dir, filename)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    import sys
    port = int(os.environ.get("PORT", 5000))

    print("=" * 70)
    print("🚀 GEDEON API - VERSION AVEC SCANNER")
    print("=" * 70)
    print(f"Port: {port}")
    print("📁 File serving route: /api/files/<path:filename>")
    print("🧪 Test route registered: /api/test-route")
    sys.stdout.flush()
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    
    # Initialiser les tables users et scanned_events
    init_user_tables()
    
    # Charger les caches au démarrage
    load_cinema_coords_cache()
    
    # Charger la base CNC des cinémas français
    load_cinemas_cnc()
    
    # Charger le mapping Allociné dynamiquement
    if ALLOCINE_AVAILABLE:
        load_allocine_departments()
    
    print("Optimisations:")
    print("  ✅ BASE CNC: 2053 cinémas français avec GPS")
    print("  ✅ MAPPING DYNAMIQUE Allociné (vrais IDs)")
    print("  ✅ Cache persistant des cinémas")
    print("  ✅ Parallélisation DATAtourisme + OpenAgenda")
    print("  ✅ Scanner avec gestion utilisateurs")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=port, debug=True)
