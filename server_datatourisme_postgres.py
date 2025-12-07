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
    cache_key = (round(lat, 2), round(lon, 2))
    if cache_key in GEOCODE_CACHE:
        return GEOCODE_CACHE[cache_key]
    
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
        print(f"   ⚠️ Erreur Nominatim: {e}")
        GEOCODE_CACHE[cache_key] = (None, None, None)
        return (None, None, None)


def geocode_address_nominatim(address_str):
    """Géocode une adresse texte."""
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


def geocode_cinema(cinema_name, cinema_address):
    """Géocode un cinéma avec cache et coordonnées connues."""
    cache_key = f"{cinema_name}:{cinema_address}"
    if cache_key in CINEMA_COORDS_CACHE:
        return CINEMA_COORDS_CACHE[cache_key]
    
    # Coordonnées connues
    name_lower = cinema_name.lower().strip()
    for known_name, coords in KNOWN_CINEMAS_GPS.items():
        if known_name in name_lower or name_lower.startswith(known_name[:10]):
            CINEMA_COORDS_CACHE[cache_key] = coords
            return coords
    
    # Géocodage Nominatim
    if cinema_address:
        lat, lon = geocode_address_nominatim(f"{cinema_address}, France")
        if lat:
            CINEMA_COORDS_CACHE[cache_key] = (lat, lon)
            save_cinema_coords_cache()
            time.sleep(0.1)
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
    🎬 VERSION OPTIMISÉE avec mapping statique et recherche élargie.
    """
    if not ALLOCINE_AVAILABLE:
        return []
    
    print(f"🎬 Cinéma optimisé: ({center_lat:.4f}, {center_lon:.4f}), {radius_km}km")
    start_time = time.time()
    
    # Charger cache
    if not CINEMA_COORDS_CACHE:
        load_cinema_coords_cache()
    
    # 1. Récupérer localisation via Nominatim
    dept_name, postcode, city = reverse_geocode_nominatim(center_lat, center_lon)
    
    if not dept_name and not postcode:
        print("   ⚠️ Localisation non trouvée")
        return []
    
    # 2. Déterminer les départements à rechercher (MAPPING STATIQUE)
    dept_ids = []
    dept_code = None
    
    if postcode:
        if postcode.upper().startswith("2A") or postcode.upper().startswith("2B"):
            dept_code = postcode[:2].upper()
        else:
            dept_code = postcode[:2]
        
        primary_id = get_allocine_dept_id_from_postcode(postcode)
        if primary_id:
            dept_ids.append(primary_id)
    
    if not dept_ids and dept_name:
        primary_id = get_allocine_dept_id(dept_name)
        if primary_id:
            dept_ids.append(primary_id)
    
    if not dept_ids:
        print(f"   ⚠️ Département non mappé: {dept_name} / {postcode}")
        return []
    
    # 3. Étendre la recherche si IDF ou grand rayon
    if is_in_idf(dept_name, postcode):
        dept_ids = IDF_DEPARTMENTS.copy()
        print(f"   📍 Zone IDF → {len(dept_ids)} départements")
    elif radius_km > 30 and dept_code and dept_code in ADJACENT_DEPARTMENTS:
        for adj_code in ADJACENT_DEPARTMENTS[dept_code]:
            adj_id = POSTCODE_TO_ALLOCINE.get(adj_code)
            if adj_id and adj_id not in dept_ids:
                dept_ids.append(adj_id)
        print(f"   📍 Rayon étendu → {len(dept_ids)} départements")
    
    # 4. Récupérer tous les cinémas
    all_cinemas = []
    for dept_id in dept_ids:
        cinemas = get_cinemas_for_department(dept_id)
        all_cinemas.extend(cinemas)
    
    print(f"   🎦 {len(all_cinemas)} cinémas trouvés")
    
    if not all_cinemas:
        return []
    
    # 5. Filtrer par distance
    nearby_cinemas = []
    seen_ids = set()
    
    print(f"   🔍 Géocodage de {len(all_cinemas)} cinémas...")
    
    for cinema in all_cinemas:
        cinema_id = cinema.get('id')
        if not cinema_id or cinema_id in seen_ids:
            continue
        seen_ids.add(cinema_id)
        
        cinema_name = cinema.get('name', '')
        cinema_address = cinema.get('address', '')
        
        lat, lon = geocode_cinema(cinema_name, cinema_address)
        
        if lat and lon:
            dist = haversine_km(center_lat, center_lon, lat, lon)
            print(f"      📍 {cinema_name}: ({lat:.4f}, {lon:.4f}) → {dist:.1f}km")
            if dist <= radius_km:
                nearby_cinemas.append({
                    'id': cinema_id,
                    'name': cinema_name,
                    'address': cinema_address,
                    'lat': lat,
                    'lon': lon,
                    'distance': dist
                })
            else:
                print(f"         ❌ Hors rayon ({dist:.1f}km > {radius_km}km)")
        else:
            print(f"      ⚠️ {cinema_name}: géocodage échoué (adresse: {cinema_address})")
    
    print(f"   📍 {len(nearby_cinemas)} cinémas dans le rayon")
    
    if not nearby_cinemas:
        return []
    
    # 6. Limiter et trier
    nearby_cinemas.sort(key=lambda c: c['distance'])
    if len(nearby_cinemas) > max_cinemas:
        nearby_cinemas = nearby_cinemas[:max_cinemas]
    
    # 7. Récupérer les films en parallèle
    today_str = date.today().strftime("%Y-%m-%d")
    all_events = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_movies_for_cinema, cinema, today_str): cinema
            for cinema in nearby_cinemas
        }
        
        for future in as_completed(futures):
            try:
                cinema_info, movies = future.result(timeout=15)
                
                if movies:
                    print(f"      🎬 {cinema_info['name']}: {len(movies)} films")
                    for movie in movies:
                        # Gestion de la durée (deux formats possibles)
                        runtime = movie.get('runtime', 0)
                        duration_str = movie.get('duration', '')  # Format texte de get_showtime
                        
                        if runtime and isinstance(runtime, int):
                            h, m = runtime // 3600, (runtime % 3600) // 60
                            duration = f"{h}h{m:02d}" if h else f"{m}min"
                        elif duration_str:
                            duration = duration_str
                        else:
                            duration = ""
                        
                        # Gestion des horaires (de get_showtime)
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
                            "city": city or dept_name or "",
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
                print(f"      ❌ Erreur future: {e}")
    
    save_cinema_coords_cache()
    
    print(f"   ✅ {len(all_events)} films en {time.time()-start_time:.1f}s")
    return all_events


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
    
    # Charger le cache au démarrage
    load_cinema_coords_cache()
    
    print("=" * 70)
    print("🚀 GEDEON API - VERSION OPTIMISÉE v2")
    print("=" * 70)
    print(f"Port: {port}")
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print("Optimisations:")
    print("  ✅ Mapping statique départements (pas d'appel API)")
    print("  ✅ Recherche par code postal (plus fiable)")
    print("  ✅ Recherche élargie IDF (8 départements)")
    print("  ✅ Départements adjacents si rayon > 30km")
    print("  ✅ Cache persistant des cinémas")
    print("  ✅ get_movies() pour données enrichies")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=port, debug=True)
