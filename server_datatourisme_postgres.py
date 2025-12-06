#!/usr/bin/env python3
"""
API Flask OPTIMISÉE pour servir les événements DATAtourisme depuis PostgreSQL
+ OpenAgenda pour une couverture complète
+ Allociné pour les séances de cinéma

🚀 OPTIMISATIONS :
- Index GIST sur geom pour requêtes spatiales ultra-rapides
- LIMIT réduit à 500 (au lieu de 2000)
- Requête SQL simplifiée avec un seul calcul de distance
- Cache Nominatim étendu
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

# Allociné API
try:
    from allocineAPI.allocineAPI import allocineAPI
    ALLOCINE_AVAILABLE = True
    print("✅ Allociné API disponible")
except ImportError:
    ALLOCINE_AVAILABLE = False
    print("⚠️ Allociné API non disponible")

# ============================================================================
# CINÉMAS PARIS - COORDONNÉES PRÉ-CALCULÉES
# ============================================================================

KNOWN_CINEMAS_GPS = {
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

# OpenAgenda
API_KEY = os.environ.get("OPENAGENDA_API_KEY", "a05c8baab2024ef494d3250fe4fec435")
BASE_URL = os.environ.get("OPENAGENDA_BASE_URL", "https://api.openagenda.com/v2")

# Valeurs par défaut
RADIUS_KM_DEFAULT = 30
DAYS_AHEAD_DEFAULT = 30

# Cache
GEOCODE_CACHE = {}
DEPARTMENT_CACHE = {}
CINEMA_CACHE = {}

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
    """Retourne le nom du département via Nominatim."""
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
        
        city = address.get("city", "")
        county = address.get("county", "")
        state_district = address.get("state_district", "")
        state = address.get("state", "")
        
        if city in ["Paris", "Lyon", "Marseille"]:
            dept_name = city
        elif county:
            dept_name = county
        elif state_district:
            dept_name = state_district
        else:
            dept_name = state
        
        GEOCODE_CACHE[cache_key] = dept_name
        return dept_name
    except Exception as e:
        print(f"❌ Reverse geocode error: {e}")
        GEOCODE_CACHE[cache_key] = None
        return None


# ============================================================================
# OPENAGENDA (code identique au serveur précédent)
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
    """Récupère les événements d'un agenda."""
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

    print(f"📚 {len(agendas)} agendas trouvés")

    all_events = []
    for agenda in agendas:
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
                "openagenda_url": openagenda_url,
                "agendaTitle": agenda_title,
                "source": "OpenAgenda"
            })

    print(f"✅ OpenAgenda: {len(all_events)} événements trouvés")
    return all_events


# ============================================================================
# ALLOCINÉ (code simplifié - fonction principale seulement)
# ============================================================================

def get_department_id_allocine(dept_name):
    """Trouve l'ID AlloCiné d'un département."""
    if not ALLOCINE_AVAILABLE:
        return None
    
    MANUAL_MAPPING = {
        'paris': ['hauts-de-seine', 'seine-saint-denis', 'val-de-marne'],
        'île-de-france': ['hauts-de-seine', 'seine-saint-denis', 'val-de-marne'],
        'lyon': ['rhône'],
        'marseille': ['bouches-du-rhône'],
        'vaucluse': ['vaucluse'],
    }
    
    if not DEPARTMENT_CACHE:
        try:
            api = allocineAPI()
            depts = api.get_departements()
            for d in depts:
                name = d.get('name', '').lower().strip()
                dept_id = d.get('id')
                DEPARTMENT_CACHE[name] = dept_id
        except Exception as e:
            print(f"❌ Erreur chargement départements: {e}")
            return None
    
    dept_lower = dept_name.lower().strip()
    
    if dept_lower in MANUAL_MAPPING:
        for pname in MANUAL_MAPPING[dept_lower]:
            if pname in DEPARTMENT_CACHE:
                return DEPARTMENT_CACHE[pname]
    
    if dept_lower in DEPARTMENT_CACHE:
        return DEPARTMENT_CACHE[dept_lower]
    
    for name, dept_id in DEPARTMENT_CACHE.items():
        if dept_lower in name or name in dept_lower:
            return dept_id
    
    return None


def find_cinema_allocine(dept_id, target_name):
    """Trouve un cinéma AlloCiné par son nom."""
    if not ALLOCINE_AVAILABLE:
        return None
    
    cache_key = f"{dept_id}:{target_name.lower()}"
    if cache_key in CINEMA_CACHE:
        return CINEMA_CACHE[cache_key]
    
    try:
        api = allocineAPI()
        cinemas = api.get_cinema(dept_id)
    except Exception as e:
        return None
    
    target = target_name.lower()
    best_match = None
    best_score = 0
    
    for cinema in cinemas:
        name = cinema.get('name', '').lower()
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


def fetch_allocine_cinemas_nearby(center_lat, center_lon, radius_km):
    """Récupère les cinémas et séances AlloCiné dans une zone."""
    if not ALLOCINE_AVAILABLE:
        return []
    
    print(f"🎬 Allociné: Recherche autour de ({center_lat}, {center_lon}), rayon={radius_km}km")
    
    try:
        api = allocineAPI()
        today = date.today().strftime("%Y-%m-%d")
        
        dept_name = reverse_geocode_department(center_lat, center_lon)
        if not dept_name:
            return []
        
        all_cinemas = []
        dept_lower = dept_name.lower().strip()
        
        # Logique de recherche selon localisation (simplifiée)
        if dept_lower in ['paris', 'île-de-france']:
            # Paris
            try:
                top_villes = api.get_top_villes()
                for ville in top_villes:
                    if "Paris" in ville.get('name', ''):
                        cinemas = api.get_cinema(ville.get('id'))
                        if cinemas:
                            all_cinemas.extend(cinemas)
                        break
            except:
                pass
            
            # Départements IDF
            idf_depts = ['hauts-de-seine', 'seine-saint-denis', 'val-de-marne']
            for dept in idf_depts:
                try:
                    dept_id = get_department_id_allocine(dept)
                    if dept_id:
                        cinemas = api.get_cinema(dept_id)
                        if cinemas:
                            all_cinemas.extend(cinemas)
                except:
                    pass
        else:
            # Autres départements
            dept_id = get_department_id_allocine(dept_name)
            if dept_id:
                try:
                    all_cinemas = api.get_cinema(dept_id)
                except:
                    pass
        
        if not all_cinemas:
            return []
        
        # Géocodage et filtrage
        nearby_cinemas = []
        for cinema in all_cinemas:
            cinema_name = cinema.get('name', '')
            cinema_address = cinema.get('address', '')
            cinema_id = cinema.get('id')
            
            if not cinema_name:
                continue
            
            name_lower = cinema_name.lower().strip()
            cinema_lat, cinema_lon = None, None
            
            # Coordonnées pré-calculées
            if name_lower in KNOWN_CINEMAS_GPS:
                cinema_lat, cinema_lon = KNOWN_CINEMAS_GPS[name_lower]
            else:
                for known_name, coords in KNOWN_CINEMAS_GPS.items():
                    if known_name in name_lower or name_lower.startswith(known_name[:10]):
                        cinema_lat, cinema_lon = coords
                        break
            
            # Géocodage si nécessaire
            if not cinema_lat and cinema_address:
                cinema_lat, cinema_lon = geocode_address_nominatim(f"{cinema_address}, France")
                time.sleep(0.1)
            
            if cinema_lat and cinema_lon:
                dist = haversine_km(center_lat, center_lon, cinema_lat, cinema_lon)
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
        
        if not nearby_cinemas:
            return []
        
        nearby_cinemas.sort(key=lambda c: c['distance'])
        
        # Récupérer les films
        all_cinema_events = []
        for cinema_info in nearby_cinemas:
            try:
                showtimes = api.get_showtime(cinema_info['id'], today)
                time.sleep(0.05)
                
                if showtimes:
                    for show in showtimes:
                        film_title = show.get('title', 'Film')
                        duration = show.get('duration', '')
                        
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
                            "uid": f"allocine-{cinema_info['id']}-{film_title[:20]}",
                            "title": f"🎬 {film_title}",
                            "begin": today,
                            "end": today,
                            "locationName": cinema_info['name'],
                            "city": dept_name,
                            "address": cinema_info['address'],
                            "latitude": cinema_info['lat'],
                            "longitude": cinema_info['lon'],
                            "distanceKm": round(cinema_info['distance'], 1),
                            "openagendaUrl": "",
                            "agendaTitle": f"Films {cinema_info['name']}",
                            "source": "Allocine",
                            "description": f"{duration} - {versions_str}"
                        }
                        all_cinema_events.append(event)
            except:
                continue
        
        return all_cinema_events
        
    except Exception as e:
        print(f"❌ Erreur Allociné: {e}")
        return []


# ============================================================================
# ROUTES - VERSION OPTIMISÉE
# ============================================================================

@app.route('/')
def index():
    """Page d'accueil"""
    return send_from_directory('.', 'index.html')


@app.route('/api/events/nearby', methods=['GET'])
def get_nearby_events():
    """
    🚀 VERSION OPTIMISÉE
    Récupère les événements à proximité (DATAtourisme + OpenAgenda)
    """
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        days_ahead = request.args.get('days', DAYS_AHEAD_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({"status": "error", "message": "Paramètres 'lat' et 'lon' requis"}), 400
        
        print(f"🔍 Recherche OPTIMISÉE: ({center_lat}, {center_lon}), rayon={radius_km}km")
        
        date_limite = datetime.now().date() + timedelta(days=days_ahead)
        all_events = []
        datatourisme_count = 0
        openagenda_count = 0
        
        # ========== DATATOURISME - REQUÊTE OPTIMISÉE ==========
        try:
            import time
            start_time = time.time()
            
            conn = get_db_connection()
            cur = conn.cursor()
            
            # 🚀 REQUÊTE OPTIMISÉE
            # - Un seul calcul de distance (pas deux)
            # - Utilise l'index GIST sur geom
            # - LIMIT réduit à 500 (au lieu de 2000)
            # - Filtre date_fin AVANT le calcul de distance
            query = """
                WITH nearby_events AS (
                    SELECT 
                        uri, nom, description,
                        date_debut, date_fin,
                        latitude, longitude, 
                        adresse, commune, code_postal, contacts,
                        geom
                    FROM evenements
                    WHERE 
                        -- Filtre temporel d'abord (utilise l'index sur date_fin)
                        (date_fin IS NULL OR date_fin >= CURRENT_DATE)
                        AND (date_debut IS NULL OR date_debut <= %s)
                        -- Puis filtre spatial (utilise l'index GIST)
                        AND ST_DWithin(
                            geom::geography,
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                            %s
                        )
                    LIMIT 500
                )
                SELECT 
                    uri as uid,
                    nom as title,
                    description,
                    date_debut as begin,
                    date_fin as end,
                    latitude,
                    longitude,
                    adresse as address,
                    commune as city,
                    code_postal as "postalCode",
                    contacts,
                    -- Un seul calcul de distance
                    ST_Distance(
                        geom::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) / 1000 as "distanceKm"
                FROM nearby_events
                ORDER BY "distanceKm", date_debut
            """
            
            cur.execute(query, (
                date_limite,
                center_lon, center_lat, radius_km * 1000,
                center_lon, center_lat
            ))
            
            rows = cur.fetchall()
            
            query_time = time.time() - start_time
            print(f"⚡ Requête DATAtourisme: {query_time:.3f}s ({len(rows)} résultats)")
            
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
            
            print(f"✅ DATAtourisme: {datatourisme_count} événements en {query_time:.3f}s")
            
        except Exception as e:
            print(f"⚠️ Erreur DATAtourisme: {e}")
            import traceback
            traceback.print_exc()
        
        # ========== OPENAGENDA ==========
        try:
            openagenda_events = fetch_openagenda_events(center_lat, center_lon, radius_km, days_ahead)
            openagenda_count = len(openagenda_events)
            all_events.extend(openagenda_events)
        except Exception as e:
            print(f"⚠️ Erreur OpenAgenda: {e}")
        
        # Tri final
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
            "sources": ["DATAtourisme (optimisé)", "OpenAgenda", "Allociné" if ALLOCINE_AVAILABLE else "Allociné (non dispo)"]
        }), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "database": "disconnected", "error": str(e)}), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    
    print("="*70)
    print("🚀 GEDEON API - OPTIMISÉE")
    print("="*70)
    print(f"Port: {port}")
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"Sources: DATAtourisme (optimisé) + OpenAgenda + Allociné")
    print("="*70)
    
    app.run(host='0.0.0.0', port=port, debug=True)
