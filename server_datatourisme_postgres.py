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

# Allociné API (allocine-seances)
try:
    from allocineAPI.allocineAPI import allocineAPI
    ALLOCINE_AVAILABLE = True
    print("✅ Allociné API (allocine-seances) disponible")
except ImportError:
    ALLOCINE_AVAILABLE = False
    print("⚠️ Allociné API non disponible (pip install allocine-seances)")

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
        
        dept_name = address.get("county") or address.get("state_district") or address.get("state")
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
    
    # Charger les départements une seule fois
    if not DEPARTMENT_CACHE:
        try:
            api = allocineAPI()
            depts = api.get_departements()
            for d in depts:
                name = d.get('name', '').lower()
                DEPARTMENT_CACHE[name] = d.get('id')
        except Exception as e:
            print(f"❌ Erreur chargement départements Allociné: {e}")
            return None
    
    # Recherche
    dept_lower = dept_name.lower()
    if dept_lower in DEPARTMENT_CACHE:
        return DEPARTMENT_CACHE[dept_lower]
    
    # Recherche partielle
    for name, dept_id in DEPARTMENT_CACHE.items():
        if dept_lower in name or name in dept_lower:
            return dept_id
    
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
    
    # Grandes villes de référence avec coordonnées
    villes_ref = {
        'Paris': (48.8566, 2.3522),
        'Marseille': (43.2965, 5.3698),
        'Lyon': (45.7640, 4.8357),
        'Toulouse': (43.6047, 1.4442),
        'Nice': (43.7102, 7.2620),
        'Nantes': (47.2184, -1.5536),
        'Strasbourg': (48.5734, 7.7521),
        'Montpellier': (43.6108, 3.8767),
        'Bordeaux': (44.8378, -0.5792),
        'Lille': (50.6292, 3.0573),
    }
    
    # Trouver la ville la plus proche
    best_ville = None
    best_dist = float('inf')
    best_coords = None
    
    for ville_name, (vlat, vlon) in villes_ref.items():
        d = haversine_km(center_lat, center_lon, vlat, vlon)
        if d < best_dist:
            best_dist = d
            best_ville = ville_name
            best_coords = (vlat, vlon)
    
    if best_dist > 100:
        print(f"⚠️ Aucune grande ville à moins de 100km")
        return []
    
    print(f"📍 Ville la plus proche: {best_ville} ({best_dist:.0f}km)")
    
    try:
        api = allocineAPI()
        today = date.today().strftime("%Y-%m-%d")
        
        # Récupérer les villes AlloCiné
        top_villes = api.get_top_villes()
        if not top_villes:
            print("❌ Impossible de récupérer les villes AlloCiné")
            return []
        
        # Trouver l'ID de la ville
        location_id = None
        location_name = None
        
        for ville in top_villes:
            ville_allocine = ville.get('name', '').lower()
            if best_ville.lower() in ville_allocine or ville_allocine in best_ville.lower():
                location_id = ville.get('id')
                location_name = ville.get('name')
                break
        
        if not location_id:
            print(f"❌ Ville {best_ville} non trouvée dans AlloCiné")
            return []
        
        # Récupérer les cinémas
        cinemas = api.get_cinema(location_id)
        if not cinemas:
            print("❌ Aucun cinéma trouvé")
            return []
        
        print(f"🎥 {len(cinemas)} cinémas trouvés")
        
        all_cinema_events = []
        
        # Limiter à 10 cinémas pour ne pas surcharger
        for cinema in cinemas[:10]:
            cinema_name = cinema.get('name', 'Cinéma')
            cinema_address = cinema.get('address', '')
            cinema_id = cinema.get('id')
            
            try:
                showtimes = api.get_showtime(cinema_id, today)
                
                if showtimes:
                    print(f"   🎬 {cinema_name}: {len(showtimes)} films")
                    
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
                        
                        all_cinema_events.append({
                            "uid": f"allocine-{cinema_id}-{film_title[:20]}",
                            "title": f"🎬 {film_title}",
                            "begin": today,
                            "end": today,
                            "locationName": cinema_name,
                            "city": location_name,
                            "address": cinema_address,
                            "latitude": best_coords[0],
                            "longitude": best_coords[1],
                            "distanceKm": round(best_dist, 1),
                            "openagendaUrl": "",
                            "agendaTitle": f"Séances {cinema_name}",
                            "source": "Allocine",
                            "description": f"{duration} - {versions_str}"
                        })
            except Exception as e:
                print(f"   ⚠️ Erreur pour {cinema_name}: {e}")
                continue
        
        print(f"✅ Allociné: {len(all_cinema_events)} séances trouvées")
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
