#!/usr/bin/env python3
"""
API Flask pour servir les événements DATAtourisme depuis PostgreSQL
+ OpenAgenda pour une couverture complète
+ Allociné pour les séances de cinéma (via allocine-seances)
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

# Allociné API (allocine-seances)
try:
    from allocineAPI.allocineAPI import allocineAPI
    ALLOCINE_AVAILABLE = True
    print("✅ Allociné API (allocine-seances) disponible")
except ImportError:
    ALLOCINE_AVAILABLE = False
    print("⚠️ Allociné API non disponible (pip install allocine-seances)")

print(f"📍 {len(KNOWN_CINEMAS_GPS)} cinémas avec coordonnées pré-calculées")

# ============================================================================
# CINÉMAS PARIS - COORDONNÉES PRÉ-CALCULÉES
# ============================================================================

KNOWN_CINEMAS_GPS = {
    # Paris intra-muros (75)
    'ugc ciné cité les halles': (48.8619, 2.3466),
    'pathé beaugrenelle': (48.8478, 2.2820),
    'mk2 bibliothèque': (48.8338, 2.3761),
    'mk2 quai de seine': (48.8840, 2.3719),
    'mk2 nation': (48.8482, 2.3969),
    'gaumont champs-élysées': (48.8698, 2.3046),
    'gaumont opéra': (48.8716, 2.3315),
    'ugc montparnasse': (48.8422, 2.3244),
    'le grand rex': (48.8707, 2.3477),
    'pathé levallois': (48.8920, 2.2883),
    'pathé boulogne': (48.8342, 2.2411),
    'pathé la villette': (48.8938, 2.3889),
}

# ============================================================================
# CONFIGURATION
# ============================================================================

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

# PostgreSQL - Support pour Render et local
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
    print(f"⚠️  Connexion locale: {DB_CONFIG['host']}")

# === OpenAgenda ===
API_KEY = os.environ.get("OPENAGENDA_API_KEY", "a05c8baab2024ef494d3250fe4fec435")
BASE_URL = os.environ.get("OPENAGENDA_BASE_URL", "https://api.openagenda.com/v2")

# Valeurs par défaut
RADIUS_KM_DEFAULT = 30
DAYS_AHEAD_DEFAULT = 30

# Cache simple en mémoire
GEOCODE_CACHE = {}
DEPARTMENT_CACHE = {}  # Cache pour les départements Allociné
CINEMA_CACHE = {}  # Cache pour les cinémas par département


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def get_db_connection():
    """Crée une connexion à PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def calculate_bounding_box(lat, lng, radius_km):
    """Calculate bounding box coordinates from a center point and radius."""
    EARTH_RADIUS_KM = 6371.0
    radius_rad = radius_km / EARTH_RADIUS_KM
    lat_rad = math.radians(lat)

    lat_delta = math.degrees(radius_rad)
    min_lat = lat - lat_delta
    max_lat = lat + lat_delta

    lng_delta = math.degrees(radius_rad / math.cos(lat_rad))
    min_lng = lng - lng_delta
    max_lng = lng + lng_delta

    return {
        'northEast': {'lat': max_lat, 'lng': max_lng},
        'southWest': {'lat': min_lat, 'lng': min_lng}
    }


def haversine_km(lat1, lon1, lat2, lon2):
    """Distance en km entre deux points (latitude/longitude)."""
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def geocode_address_nominatim(address_str):
    """Géocode une adresse texte avec Nominatim (OpenStreetMap)."""
    if not address_str:
        return None, None

    if address_str in GEOCODE_CACHE:
        return GEOCODE_CACHE[address_str]

    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address_str, "format": "json", "limit": 1}
    headers = {"User-Agent": "gedeon-events-api/1.0 (eric@ericmahe.com)"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            GEOCODE_CACHE[address_str] = (None, None)
            return None, None

        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        GEOCODE_CACHE[address_str] = (lat, lon)
        return lat, lon
    except Exception as e:
        print(f"❌ Nominatim error: {e}")
        GEOCODE_CACHE[address_str] = (None, None)
        return None, None


def reverse_geocode_department(lat, lon):
    """Retourne le nom du département via Nominatim pour un point GPS."""
    cache_key = (round(lat, 2), round(lon, 2))
    if cache_key in GEOCODE_CACHE:
        return GEOCODE_CACHE[cache_key]

    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json", "zoom": 10, "addressdetails": 1}
    headers = {"User-Agent": "gedeon-events-api/1.0 (eric@ericmahe.com)"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        address = data.get("address", {})
        
        # Priorités d'extraction :
        # 1. city (pour Paris, Lyon, Marseille qui sont ville+département)
        # 2. county (département classique)
        # 3. state_district (fallback)
        # 4. state (région, dernier recours)
        city = address.get("city", "")
        county = address.get("county", "")
        state_district = address.get("state_district", "")
        state = address.get("state", "")
        
        # Cas spéciaux : grandes villes = départements
        if city in ["Paris", "Lyon", "Marseille"]:
            dept_name = city
        elif county:
            dept_name = county
        elif state_district:
            dept_name = state_district
        else:
            dept_name = state
        
        print(f"🗺️ Nominatim: city={city}, county={county}, state_district={state_district}, state={state} → {dept_name}")
        
        GEOCODE_CACHE[cache_key] = dept_name
        return dept_name
    except Exception as e:
        print(f"❌ Reverse geocode error: {e}")
        GEOCODE_CACHE[cache_key] = None
        return None


# ============================================================================
# OPENAGENDA
# ============================================================================

def search_agendas(search_term=None, official=None, limit=100):
    """Recherche d'agendas OpenAgenda."""
    url = f"{BASE_URL}/agendas"
    params = {"key": API_KEY, "size": min(limit, 100)}

    if search_term:
        params["search"] = search_term
    if official is not None:
        params["official"] = 1 if official else 0

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json() or {}
    except requests.exceptions.RequestException as e:
        print(f"❌ Error searching agendas: {e}")
        return {"agendas": []}


def get_events_from_agenda(agenda_uid, center_lat, center_lon, radius_km, days_ahead, limit=100):
    """Récupère les événements d'un agenda avec filtrage géographique et temporel."""
    url = f"{BASE_URL}/agendas/{agenda_uid}/events"
    bbox = calculate_bounding_box(center_lat, center_lon, radius_km)
    
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    end_date = today + timedelta(days=days_ahead)
    end_date_str = end_date.strftime('%Y-%m-%d')

    params = {
        'key': API_KEY,
        'size': min(limit, 100),
        'detailed': 1,
        'geo[northEast][lat]': bbox['northEast']['lat'],
        'geo[northEast][lng]': bbox['northEast']['lng'],
        'geo[southWest][lat]': bbox['southWest']['lat'],
        'geo[southWest][lng]': bbox['southWest']['lng'],
        'timings[gte]': today_str,
        'timings[lte]': end_date_str,
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json() or {}
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching events from agenda {agenda_uid}: {e}")
        return {"events": []}


def fetch_openagenda_events(center_lat, center_lon, radius_km, days_ahead):
    """Récupère tous les événements OpenAgenda dans la zone."""
    print(f"🔍 OpenAgenda: Recherche autour de ({center_lat}, {center_lon}), rayon={radius_km}km")

    agendas_result = search_agendas(limit=100)
    agendas = agendas_result.get('agendas', []) if agendas_result else []
    
    if not agendas:
        print("⚠️ Aucun agenda OpenAgenda trouvé")
        return []

    print(f"📚 {len(agendas)} agendas OpenAgenda trouvés")

    all_events = []
    for idx, agenda in enumerate(agendas):
        uid = agenda.get('uid')
        agenda_slug = agenda.get('slug')
        title = agenda.get('title', {})
        agenda_title = title.get('fr') or title.get('en') or 'Agenda' if isinstance(title, dict) else (title or 'Agenda')

        events_data = get_events_from_agenda(uid, center_lat, center_lon, radius_km, days_ahead)
        events = events_data.get('events', []) if events_data else []

        for ev in events:
            timings = ev.get('timings') or []
            begin_str = timings[0].get('begin') if timings else None
            end_str = timings[0].get('end') if timings else None

            loc = ev.get('location') or {}
            ev_lat = loc.get('latitude')
            ev_lon = loc.get('longitude')

            if ev_lat is None or ev_lon is None:
                parts = [loc.get("name"), loc.get("address"), loc.get("city"), "France"]
                address_str = ", ".join([p for p in parts if p])
                ev_lat, ev_lon = geocode_address_nominatim(address_str)
                if ev_lat is None:
                    continue

            try:
                ev_lat = float(ev_lat)
                ev_lon = float(ev_lon)
            except (ValueError, TypeError):
                continue

            dist = haversine_km(center_lat, center_lon, ev_lat, ev_lon)
            if dist > radius_km:
                continue

            title_field = ev.get('title')
            ev_title = title_field.get('fr') or title_field.get('en') or 'Événement' if isinstance(title_field, dict) else (title_field or 'Événement')

            event_slug = ev.get('slug')
            openagenda_url = f"https://openagenda.com/{agenda_slug}/events/{event_slug}?lang=fr" if agenda_slug and event_slug else None

            all_events.append({
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

    print(f"✅ OpenAgenda: {len(all_events)} événements trouvés")
    return all_events


# ============================================================================
# ALLOCINÉ (allocine-seances)
# ============================================================================

def get_department_id_allocine(dept_name):
    """Trouve l'ID AlloCiné d'un département."""
    if not ALLOCINE_AVAILABLE:
        return None
    
    # Mapping manuel - ATTENTION: AlloCiné n'a pas "Paris" mais les départements IDF
    MANUAL_MAPPING = {
        'paris': ['hauts-de-seine', 'seine-saint-denis', 'val-de-marne'],  # Paris → départements voisins
        'île-de-france': ['hauts-de-seine', 'seine-saint-denis', 'val-de-marne'],
        'lyon': ['rhône'],
        'marseille': ['bouches-du-rhône'],
        'vaucluse': ['vaucluse'],
    }
    
    # Charger les départements une seule fois
    if not DEPARTMENT_CACHE:
        print("🔄 Chargement des départements AlloCiné...")
        try:
            api = allocineAPI()
            depts = api.get_departements()
            for d in depts:
                name = d.get('name', '').lower().strip()
                dept_id = d.get('id')
                DEPARTMENT_CACHE[name] = dept_id
            print(f"✅ {len(DEPARTMENT_CACHE)} départements chargés")
        except Exception as e:
            print(f"❌ Erreur chargement départements Allociné: {e}")
            return None
    
    dept_lower = dept_name.lower().strip()
    
    # 1. Recherche via mapping manuel - retourne le PREMIER département trouvé
    if dept_lower in MANUAL_MAPPING:
        possible_names = MANUAL_MAPPING[dept_lower]
        for pname in possible_names:
            if pname in DEPARTMENT_CACHE:
                print(f"✅ Mapping manuel: '{dept_name}' → '{pname}' (ID: {DEPARTMENT_CACHE[pname]})")
                return DEPARTMENT_CACHE[pname]
    
    # 2. Recherche exacte
    if dept_lower in DEPARTMENT_CACHE:
        return DEPARTMENT_CACHE[dept_lower]
    
    # 3. Recherche partielle
    for name, dept_id in DEPARTMENT_CACHE.items():
        if dept_lower in name or name in dept_lower:
            print(f"✅ Matching partiel: '{dept_name}' → '{name}' (ID: {dept_id})")
            return dept_id
    
    print(f"❌ Département '{dept_name}' non trouvé.")
    print(f"   Départements disponibles: essonne, hauts-de-seine, seine-saint-denis, val-de-marne, yvelines, etc.")
    return None


def find_cinema_allocine(dept_id, target_name):
    """Trouve un cinéma AlloCiné par son nom dans un département."""
    if not ALLOCINE_AVAILABLE:
        return None
    
    cache_key = f"{dept_id}:{target_name.lower()}"
    if cache_key in CINEMA_CACHE:
        return CINEMA_CACHE[cache_key]
    
    try:
        api = allocineAPI()
        cinemas = api.get_cinema(dept_id)
    except Exception as e:
        print(f"❌ Erreur recherche cinémas: {e}")
        return None
    
    target = target_name.lower()
    best_match = None
    best_score = 0
    
    for cinema in cinemas:
        name = cinema.get('name', '').lower()
        
        # Score de correspondance
        score = 0
        if target == name:
            score = 100
        elif target in name or name in target:
            score = 50
        else:
            target_words = set(target.split())
            name_words = set(name.split())
            common = len(target_words & name_words)
            score = common * 10
        
        if score > best_score:
            best_score = score
            best_match = cinema
    
    if best_match and best_score >= 20:
        CINEMA_CACHE[cache_key] = best_match
        return best_match
    
    return None


def fetch_allocine_showtimes(cinema_name, cinema_lat, cinema_lon, date_str=None):
    """Récupère les séances AlloCiné pour un cinéma."""
    if not ALLOCINE_AVAILABLE:
        return []
    
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")
    
    # 1. Déterminer le département
    dept_name = reverse_geocode_department(cinema_lat, cinema_lon)
    if not dept_name:
        print(f"⚠️ Impossible de déterminer le département pour {cinema_name}")
        return []
    
    print(f"🗺️ Département: {dept_name}")
    
    # 2. Trouver l'ID du département AlloCiné
    dept_id = get_department_id_allocine(dept_name)
    if not dept_id:
        print(f"⚠️ Département AlloCiné non trouvé pour '{dept_name}'")
        return []
    
    # 3. Trouver le cinéma correspondant
    cinema = find_cinema_allocine(dept_id, cinema_name)
    if not cinema:
        print(f"⚠️ Cinéma AlloCiné non trouvé: '{cinema_name}'")
        return []
    
    cinema_id = cinema['id']
    print(f"🎬 Cinéma trouvé: {cinema['name']} (ID: {cinema_id})")
    
    # 4. Récupérer les séances
    try:
        api = allocineAPI()
        showtimes = api.get_showtime(cinema_id, date_str)
        print(f"🎞️ {len(showtimes)} films avec séances")
        return showtimes
    except Exception as e:
        print(f"❌ Erreur récupération séances: {e}")
        return []


def fetch_allocine_cinemas_nearby(center_lat, center_lon, radius_km):
    """Récupère les cinémas et séances AlloCiné dans une zone."""
    if not ALLOCINE_AVAILABLE:
        print("⚠️ Allociné non disponible")
        return []
    
    print(f"🎬 Allociné: Recherche autour de ({center_lat}, {center_lon}), rayon={radius_km}km")
    
    try:
        api = allocineAPI()
        today = date.today().strftime("%Y-%m-%d")
        
        # 1. DÉTERMINER LE DÉPARTEMENT à partir des coordonnées GPS
        dept_name = reverse_geocode_department(center_lat, center_lon)
        if not dept_name:
            print("⚠️ Impossible de déterminer le département")
            return []
        
        print(f"📍 Département détecté: {dept_name}")
        
        # 2. DÉPARTEMENTS À CHERCHER (cas spécial Paris = plusieurs départements)
        dept_ids_to_search = []
        dept_lower = dept_name.lower().strip()
        
        # Cas spécial: Paris = chercher dans tous les départements IDF proches
        if dept_lower in ['paris', 'île-de-france']:
            print("🏙️ Paris détecté → recherche dans les départements limitrophes")
            idf_depts = ['hauts-de-seine', 'seine-saint-denis', 'val-de-marne', 
                         'seine-et-marne', 'yvelines', 'essonne', 'val-d\'oise']
            for d in idf_depts:
                dept_id = get_department_id_allocine(d)
                if dept_id:
                    dept_ids_to_search.append((d, dept_id))
        else:
            # Cas normal: un seul département
            dept_id = get_department_id_allocine(dept_name)
            if dept_id:
                dept_ids_to_search.append((dept_name, dept_id))
        
        if not dept_ids_to_search:
            print(f"⚠️ Aucun département AlloCiné trouvé pour '{dept_name}'")
            return []
        
        print(f"🔍 Recherche dans {len(dept_ids_to_search)} département(s)")
        
        # 3. RÉCUPÉRER TOUS les cinémas des départements
        all_cinemas = []
        for dept_label, dept_id in dept_ids_to_search:
            try:
                cinemas = api.get_cinema(dept_id)
                if cinemas:
                    print(f"   📍 {dept_label}: {len(cinemas)} cinémas")
                    all_cinemas.extend(cinemas)
            except Exception as e:
                print(f"   ⚠️ Erreur {dept_label}: {e}")
        
        if not all_cinemas:
            print("❌ Aucun cinéma trouvé")
            return []
        
        print(f"🎥 {len(all_cinemas)} cinémas au total")
        
        # 4. PRIORISER les cinémas (pour Paris : trier par code postal proche)
        if dept_lower in ['paris', 'île-de-france'] and len(all_cinemas) > 50:
            print(f"🏙️ Paris détecté : priorisation par proximité...")
            # Extraire codes postaux et calculer distance approximative
            def get_priority_score(cinema):
                addr = cinema.get('address', '').lower()
                # Chercher code postal Paris intra-muros (75xxx)
                if '75' in addr and 'paris' in addr:
                    return 1  # Priorité max
                # Hauts-de-Seine proche (92xxx)
                elif '92' in addr or 'boulogne' in addr or 'neuilly' in addr:
                    return 2
                # Val-de-Marne/Seine-Saint-Denis proche
                elif '93' in addr or '94' in addr:
                    return 3
                else:
                    return 10  # Loin
            
            all_cinemas.sort(key=get_priority_score)
            print(f"   ✓ {len([c for c in all_cinemas if get_priority_score(c) <= 3])} cinémas prioritaires")
        
        # 5. FILTRER par distance et trier par proximité
        nearby_cinemas = []
        geocoded_count = 0
        max_geocode = 30  # Augmenter à 30 pour Paris
        
        print(f"🔍 Géocodage des cinémas (max {max_geocode}/{len(all_cinemas)})...")
        
        for cinema in all_cinemas:
            if geocoded_count >= max_geocode:
                print(f"   ⚠️ Limite atteinte ({max_geocode} cinémas géocodés)")
                break
                
            cinema_name = cinema.get('name', '')
            cinema_address = cinema.get('address', '')
            cinema_id = cinema.get('id')
            
            if not cinema_name:
                continue
            
            # 1. ESSAYER d'abord les coordonnées pré-calculées
            name_lower = cinema_name.lower().strip()
            cinema_lat, cinema_lon = None, None
            
            # Chercher correspondance exacte
            if name_lower in KNOWN_CINEMAS_GPS:
                cinema_lat, cinema_lon = KNOWN_CINEMAS_GPS[name_lower]
                print(f"   ✓ {cinema_name}: coordonnées pré-calculées")
            else:
                # Chercher correspondance partielle
                for known_name, coords in KNOWN_CINEMAS_GPS.items():
                    if known_name in name_lower or name_lower.startswith(known_name[:10]):
                        cinema_lat, cinema_lon = coords
                        print(f"   ✓ {cinema_name}: match partiel '{known_name}'")
                        break
            
            # 2. Si pas trouvé, GÉOCODER avec Nominatim
            if not cinema_lat and cinema_address:
                full_address = f"{cinema_address}, France"
                cinema_lat, cinema_lon = geocode_address_nominatim(full_address)
                geocoded_count += 1
                time.sleep(0.1)  # Rate limit
            
            # 3. Si on a des coordonnées (pré-calc ou géocodées), calculer distance
            if cinema_lat and cinema_lon:
                    # Calculer la distance
                    dist = haversine_km(center_lat, center_lon, cinema_lat, cinema_lon)
                    
                    # Garder uniquement les cinémas dans le rayon
                    if dist <= radius_km:
                        nearby_cinemas.append({
                            'cinema': cinema,
                            'id': cinema_id,
                            'name': cinema_name,
                            'address': cinema_address,
                            'lat': cinema_lat,
                            'lon': cinema_lon,
                            'distance': dist
                        })
                
                # Petit délai pour respecter rate limit Nominatim (1 req/sec)
                import time
                time.sleep(0.1)  # 100ms entre chaque requête
        
        if not nearby_cinemas:
            print(f"❌ Aucun cinéma trouvé dans un rayon de {radius_km}km")
            return []
        
        # 5. TRIER par distance (les plus proches d'abord)
        nearby_cinemas.sort(key=lambda c: c['distance'])
        
        print(f"✅ {len(nearby_cinemas)} cinémas dans le rayon de {radius_km}km")
        for i, c in enumerate(nearby_cinemas[:5]):
            print(f"   {i+1}. {c['name']}: {c['distance']:.1f}km")
        
        # 6. RÉCUPÉRER LES SÉANCES pour chaque cinéma (limiter à 10 max)
        all_cinema_events = []
        cinemas_with_showtimes = 0
        
        for cinema_info in nearby_cinemas[:10]:  # Maximum 10 cinémas
            cinema_id = cinema_info['id']
            cinema_name = cinema_info['name']
            cinema_lat = cinema_info['lat']
            cinema_lon = cinema_info['lon']
            cinema_dist = cinema_info['distance']
            
            try:
                showtimes = api.get_showtime(cinema_id, today)
                
                if showtimes:
                    cinemas_with_showtimes += 1
                    print(f"   🎬 {cinema_name} ({cinema_dist:.1f}km): {len(showtimes)} films")
                    
                    for show in showtimes:
                        film_title = show.get('title', 'Film')
                        duration = show.get('duration', '')
                        
                        # Formater les horaires
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
                        
                        versions_str = " | ".join(versions) if versions else "Horaires non disponibles"
                        
                        event = {
                            "uid": f"allocine-{cinema_id}-{film_title[:20]}",
                            "title": f"🎬 {film_title}",
                            "begin": today,
                            "end": today,
                            "locationName": cinema_name,
                            "city": dept_name,
                            "address": cinema_info['address'],
                            "latitude": cinema_lat,
                            "longitude": cinema_lon,
                            "distanceKm": round(cinema_dist, 1),
                            "openagendaUrl": "",
                            "agendaTitle": f"Séances {cinema_name}",
                            "source": "Allocine",
                            "description": f"{duration} - {versions_str}"
                        }
                        all_cinema_events.append(event)
                        print(f"      + {film_title[:30]}")
                    
            except Exception as e:
                print(f"   ❌ Erreur pour {cinema_name}: {e}")
                continue
        
        print(f"✅ Allociné: {len(all_cinema_events)} séances trouvées dans {cinemas_with_showtimes} cinémas")
        return all_cinema_events
        
    except Exception as e:
        print(f"❌ Erreur Allociné: {e}")
        import traceback
        traceback.print_exc()
        return []


# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
def index():
    """Page d'accueil"""
    return send_from_directory('.', 'index.html')


@app.route('/api/events/nearby', methods=['GET'])
def get_nearby_events():
    """Récupère les événements à proximité (DATAtourisme + OpenAgenda)"""
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        days_ahead = request.args.get('days', DAYS_AHEAD_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({"status": "error", "message": "Paramètres 'lat' et 'lon' requis"}), 400
        
        print(f"🔍 Recherche: ({center_lat}, {center_lon}), rayon={radius_km}km, jours={days_ahead}")
        
        date_limite = datetime.now().date() + timedelta(days=days_ahead)
        all_events = []
        datatourisme_count = 0
        openagenda_count = 0
        
        # 1. DATAtourisme (PostgreSQL)
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            query = """
                SELECT 
                    uri as uid, nom as title, description,
                    date_debut as begin, date_fin as end,
                    latitude, longitude, adresse as address, commune as city,
                    code_postal as "postalCode", contacts,
                    ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000 as "distanceKm"
                FROM evenements
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
                AND (date_debut IS NULL OR date_debut <= %s)
                AND (date_fin IS NULL OR date_fin >= CURRENT_DATE)
                ORDER BY "distanceKm", date_debut
                LIMIT 2000
            """
            
            cur.execute(query, (center_lon, center_lat, center_lon, center_lat, radius_km * 1000, date_limite))
            rows = cur.fetchall()
            
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
                
                all_events.append(event)
            
            datatourisme_count = len(rows)
            cur.close()
            conn.close()
            print(f"✅ DATAtourisme: {datatourisme_count} événements")
        except Exception as e:
            print(f"⚠️ Erreur DATAtourisme: {e}")
        
        # 2. OpenAgenda
        try:
            openagenda_events = fetch_openagenda_events(center_lat, center_lon, radius_km, days_ahead)
            openagenda_count = len(openagenda_events)
            all_events.extend(openagenda_events)
        except Exception as e:
            print(f"⚠️ Erreur OpenAgenda: {e}")
        
        # Tri par distance
        all_events.sort(key=lambda e: (e.get("distanceKm") or 999, e.get("begin") or ""))
        
        print(f"✅ Total: {len(all_events)} événements")
        
        return jsonify({
            "status": "success",
            "center": {"latitude": center_lat, "longitude": center_lon},
            "radiusKm": radius_km,
            "days": days_ahead,
            "events": all_events,
            "count": len(all_events),
            "sources": {"DATAtourisme": datatourisme_count, "OpenAgenda": openagenda_count}
        }), 200
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/cinema/nearby', methods=['GET'])
def get_nearby_cinema():
    """Récupère les séances de cinéma AlloCiné"""
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
    """Statistiques de la base"""
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
            "sources": ["DATAtourisme", "OpenAgenda", "Allociné"]
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "sources": ["DATAtourisme", "OpenAgenda", "Allociné" if ALLOCINE_AVAILABLE else "Allociné (non dispo)"]
        }), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "database": "disconnected", "error": str(e)}), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    
    print("="*70)
    print("🚀 GEDEON API - ÉVÉNEMENTS CULTURELS FRANCE")
    print("="*70)
    print(f"Port: {port}")
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"Sources: DATAtourisme + OpenAgenda + {'Allociné' if ALLOCINE_AVAILABLE else 'Allociné (non dispo)'}")
    print("="*70)
    
    app.run(host='0.0.0.0', port=port, debug=True)
