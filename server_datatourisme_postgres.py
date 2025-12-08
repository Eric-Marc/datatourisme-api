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

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from datetime import datetime, timezone, timedelta, date
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from urllib.parse import urlparse
import requests
import math
import time
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

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
database_url = os.environ.get('DATABASE_URL')

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
        
        query = """
            WITH nearby_events AS (
                SELECT uri, nom, description, date_debut, date_fin,
                       latitude, longitude, adresse, commune, code_postal, contacts, geom
                FROM evenements
                WHERE (date_fin IS NULL OR date_fin >= CURRENT_DATE)
                  AND (date_debut IS NULL OR date_debut <= %s)
                  AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
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
        
        cur.execute(query, (date_limite, center_lon, center_lat, radius_km * 1000, center_lon, center_lat))
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
            begin_str = timings[0].get('begin') if timings else None
            end_str = timings[0].get('end') if timings else None
            
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


def fetch_movies_for_cinema(cinema_info, today_str):
    """Worker pour récupérer les films d'un cinéma."""
    try:
        api = allocineAPI()
        cinema_id = cinema_info['id']
        
        # Essayer d'abord get_showtime (plus fiable)
        try:
            showtimes = api.get_showtime(cinema_id, today_str)
            if showtimes:
                # Convertir showtimes en format compatible
                movies = []
                for show in showtimes:
                    # Extraire les horaires
                    vf = show.get('VF', [])
                    vo = show.get('VO', [])
                    vost = show.get('VOST', [])
                    
                    versions = []
                    if vf:
                        versions.append(f"VF: {', '.join(vf[:3])}")
                    if vo:
                        versions.append(f"VO: {', '.join(vo[:3])}")
                    if vost:
                        versions.append(f"VOST: {', '.join(vost[:3])}")
                    
                    movies.append({
                        'title': show.get('title', 'Film'),
                        'runtime': 0,  # Non disponible avec get_showtime
                        'genres': [],
                        'urlPoster': '',
                        'director': '',
                        'isPremiere': False,
                        'weeklyOuting': False,
                        'showtimes_str': " | ".join(versions) if versions else "Horaires non disponibles",
                        'duration': show.get('duration', ''),
                    })
                return cinema_info, movies
        except Exception as e:
            print(f"      ⚠️ get_showtime({cinema_id}) failed: {e}")
        
        # Fallback sur get_movies (données enrichies mais moins fiable)
        try:
            movies = api.get_movies(cinema_id, today_str)
            if movies:
                return cinema_info, movies
        except Exception as e:
            print(f"      ⚠️ get_movies({cinema_id}) failed: {e}")
        
        return cinema_info, []
        
    except Exception as e:
        print(f"      ❌ Erreur cinéma {cinema_info.get('name')}: {e}")
        return cinema_info, []


def fetch_allocine_cinemas_nearby(center_lat, center_lon, radius_km, max_cinemas=50):
    """
    🎬 VERSION OPTIMISÉE - Recherche spatiale dans la base CNC d'abord
    
    Nouveau flux:
    1. Base CNC: Trouver tous les cinémas dans le rayon (recherche GPS directe)
    2. Pour chaque cinéma CNC → chercher correspondance Allociné par département
    3. Récupérer les films
    
    Avantage: Pas de géocodage, pas de cinémas hors rayon, plus rapide !
    """
    if not ALLOCINE_AVAILABLE:
        return []
    
    print(f"🎬 Cinéma (recherche spatiale CNC): ({center_lat:.4f}, {center_lon:.4f}), {radius_km}km")
    start_time = time.time()
    
    # Charger la base CNC si pas encore fait
    if not CINEMAS_CNC_DATA:
        load_cinemas_cnc()
    
    if not CINEMAS_CNC_DATA:
        print("   ⚠️ Base CNC non disponible")
        return []
    
    # 1. Recherche spatiale dans la base CNC (instantané)
    nearby_cnc_cinemas = []
    for cinema in CINEMAS_CNC_DATA:
        lat = cinema.get('lat')
        lon = cinema.get('lon')
        if not lat or not lon:
            continue
        
        dist = haversine_km(center_lat, center_lon, lat, lon)
        if dist <= radius_km:
            nearby_cnc_cinemas.append({
                'nom': cinema['nom'],
                'nom_normalized': cinema['nom_normalized'],
                'keywords': cinema.get('keywords', []),
                'commune': cinema.get('commune', ''),
                'dept': cinema.get('dept', ''),
                'lat': lat,
                'lon': lon,
                'distance': dist
            })
    
    nearby_cnc_cinemas.sort(key=lambda c: c['distance'])
    print(f"   📍 Base CNC: {len(nearby_cnc_cinemas)} cinémas dans le rayon de {radius_km}km")
    
    if not nearby_cnc_cinemas:
        return []
    
    # Limiter le nombre
    if len(nearby_cnc_cinemas) > max_cinemas:
        nearby_cnc_cinemas = nearby_cnc_cinemas[:max_cinemas]
    
    # Afficher les cinémas trouvés
    for c in nearby_cnc_cinemas:
        print(f"      📍 {c['nom']} ({c['commune']}): {c['distance']:.1f}km")
    
    # 2. Charger le mapping Allociné si pas encore fait
    if not ALLOCINE_DEPT_MAPPING_LOADED:
        load_allocine_departments()
    
    # 3. Récupérer les cinémas Allociné des départements concernés
    dept_codes = set(c['dept'] for c in nearby_cnc_cinemas if c.get('dept'))
    print(f"   🗺️ Départements concernés: {dept_codes}")
    
    allocine_cinemas = []
    for dept_code in dept_codes:
        dept_name = get_dept_name_from_code(dept_code)
        if dept_name:
            dept_id = get_allocine_dept_id_dynamic(dept_name)
            if dept_id:
                cinemas = get_cinemas_for_department(dept_id)
                allocine_cinemas.extend(cinemas)
                print(f"      🎦 {dept_name} ({dept_code}): {len(cinemas)} cinémas Allociné")
    
    print(f"   🎦 Total Allociné: {len(allocine_cinemas)} cinémas")
    
    # 4. Faire la correspondance CNC → Allociné
    matched_cinemas = []
    
    for cnc_cinema in nearby_cnc_cinemas:
        best_match = find_allocine_match(cnc_cinema, allocine_cinemas)
        
        if best_match:
            matched_cinemas.append({
                'id': best_match.get('id'),
                'name': cnc_cinema['nom'],
                'address': best_match.get('address', ''),
                'lat': cnc_cinema['lat'],
                'lon': cnc_cinema['lon'],
                'distance': cnc_cinema['distance']
            })
            print(f"      ✅ {cnc_cinema['nom']} → Allociné ID {best_match.get('id')}")
        else:
            print(f"      ⚠️ {cnc_cinema['nom']}: pas de correspondance Allociné")
    
    print(f"   🎯 {len(matched_cinemas)} cinémas avec correspondance Allociné")
    
    if not matched_cinemas:
        return []
    
    # 5. Récupérer les films - SÉQUENTIELLEMENT pour éviter le rate limit 429
    today_str = date.today().strftime("%Y-%m-%d")
    all_events = []
    
    # Limiter à 20 cinémas max pour éviter trop de requêtes
    cinemas_to_fetch = matched_cinemas[:20]
    
    print(f"   🎬 Récupération des films pour {len(cinemas_to_fetch)} cinémas...")
    
    for i, cinema in enumerate(cinemas_to_fetch):
        try:
            cinema_info, movies = fetch_movies_for_cinema(cinema, today_str)
            
            if movies:
                print(f"      🎬 {cinema_info['name']}: {len(movies)} films")
                for movie in movies:
                        # Gestion de la durée
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
            else:
                print(f"      ⚠️ {cinema_info['name']}: 0 films")
        except Exception as e:
            print(f"      ❌ Erreur cinéma {cinema.get('name', '?')}: {e}")
        
        # Délai entre les requêtes pour éviter le rate limit 429
        if i < len(cinemas_to_fetch) - 1:
            time.sleep(0.3)
    
    print(f"   ✅ {len(all_events)} films en {time.time()-start_time:.1f}s")
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
    """Cinémas à proximité (Allociné optimisé)."""
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({"status": "error", "message": "Paramètres 'lat' et 'lon' requis"}), 400
        
        cinema_events = fetch_allocine_cinemas_nearby(center_lat, center_lon, radius_km)
        
        return jsonify({
            "status": "success",
            "center": {"latitude": center_lat, "longitude": center_lon},
            "radiusKm": radius_km,
            "events": cinema_events,
            "count": len(cinema_events),
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
                SALONS_DATA = json.load(f)
            print(f"✅ Salons chargés: {len(SALONS_DATA)}")
        else:
            print(f"⚠️ Fichier salons_france.json non trouvé")
    except Exception as e:
        print(f"❌ Erreur chargement salons: {e}")


def parse_salon_date(date_str):
    """Parse une date de salon (format DD/MM/YYYY)."""
    try:
        from datetime import datetime
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    except:
        return None


@app.route('/api/salons/nearby', methods=['GET'])
def get_nearby_salons():
    """Salons à proximité."""
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        days_ahead = request.args.get('days', 365, type=int)  # Par défaut 1 an
        
        if center_lat is None or center_lon is None:
            return jsonify({"status": "error", "message": "Paramètres 'lat' et 'lon' requis"}), 400
        
        # Charger les salons si pas encore fait
        if not SALONS_DATA:
            load_salons_data()
        
        today = date.today()
        max_date = today + timedelta(days=days_ahead)
        
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
            
            # Filtrer par date
            salon_date = parse_salon_date(salon.get('dates', ''))
            if salon_date:
                if salon_date < today or salon_date > max_date:
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
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    
    print("=" * 70)
    print("🚀 GEDEON API - VERSION AVEC BASE CNC")
    print("=" * 70)
    print(f"Port: {port}")
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    
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
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=port, debug=True)
