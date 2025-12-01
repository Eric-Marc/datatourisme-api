#!/usr/bin/env python3
"""
API Flask pour servir les événements DATAtourisme depuis PostgreSQL
+ OpenAgenda pour une couverture complète
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from urllib.parse import urlparse
import requests
import math
import time

# Allociné API (scraping)
try:
    from allocineAPI.allocineAPI import allocineAPI
    ALLOCINE_AVAILABLE = True
    print("✅ Allociné API disponible")
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

# === OpenAgenda (copié de server.py Gedeon qui fonctionne) ===
API_KEY = os.environ.get("OPENAGENDA_API_KEY", "a05c8baab2024ef494d3250fe4fec435")
BASE_URL = os.environ.get("OPENAGENDA_BASE_URL", "https://api.openagenda.com/v2")

# Valeurs par défaut
RADIUS_KM_DEFAULT = 30
DAYS_AHEAD_DEFAULT = 30

# Cache simple en mémoire pour les géocodages Nominatim
GEOCODE_CACHE = {}


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def get_db_connection():
    """Crée une connexion à PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


# ============================================================================
# FONCTIONS OPENAGENDA (copiées de server.py Gedeon qui fonctionne)
# ============================================================================

def calculate_bounding_box(lat, lng, radius_km):
    """
    Calculate bounding box coordinates from a center point and radius.
    """
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


def search_agendas(search_term=None, official=None, limit=100):
    """
    Recherche d'agendas OpenAgenda.
    """
    url = f"{BASE_URL}/agendas"
    params = {
        "key": API_KEY,
        "size": min(limit, 100)  # Maximum 100 par l'API
    }

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
    """
    Récupère les événements d'un agenda avec filtrage géographique et temporel via l'API.
    """
    url = f"{BASE_URL}/agendas/{agenda_uid}/events"

    bbox = calculate_bounding_box(center_lat, center_lon, radius_km)
    
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    end_date = today + timedelta(days=days_ahead)
    end_date_str = end_date.strftime('%Y-%m-%d')

    params = {
        'key': API_KEY,
        'size': min(limit, 100),  # Maximum 100 par l'API
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


def geocode_address_nominatim(address_str):
    """
    Géocode une adresse texte avec Nominatim (OpenStreetMap).
    """
    if not address_str:
        return None, None

    if address_str in GEOCODE_CACHE:
        return GEOCODE_CACHE[address_str]

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address_str,
        "format": "json",
        "limit": 1
    }
    headers = {
        "User-Agent": "datatourisme-openagenda-api/1.0 (eric@ericmahe.com)"
    }

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
        print(f"🌍 Nominatim geocode OK: '{address_str}' -> ({lat}, {lon})")
        return lat, lon
    except requests.RequestException as e:
        print(f"❌ Nominatim error for '{address_str}': {e}")
        GEOCODE_CACHE[address_str] = (None, None)
        return None, None
    except (KeyError, ValueError) as e:
        print(f"❌ Nominatim parse error for '{address_str}': {e}")
        GEOCODE_CACHE[address_str] = (None, None)
        return None, None


def fetch_openagenda_events(center_lat, center_lon, radius_km, days_ahead):
    """
    Récupère TOUS les événements OpenAgenda dans la zone.
    Basé sur le code server.py qui fonctionne.
    """
    print(f"🔍 OpenAgenda: Recherche autour de ({center_lat}, {center_lon}), rayon={radius_km}km, jours={days_ahead}")
    print(f"   API_KEY: {API_KEY[:10]}...")
    print(f"   BASE_URL: {BASE_URL}")

    # 1. Recherche d'agendas (tous les agendas accessibles à la clé API)
    agendas_result = search_agendas(limit=100)
    print(f"   Résultat search_agendas: {type(agendas_result)}, clés: {agendas_result.keys() if agendas_result else 'None'}")
    
    agendas = agendas_result.get('agendas', []) if agendas_result else []
    total_agendas = len(agendas)

    print(f"📚 OpenAgenda: {total_agendas} agendas trouvés")
    
    if total_agendas > 0:
        print(f"   Premier agenda: {agendas[0].get('title', 'Sans titre')} (uid: {agendas[0].get('uid')})")

    if not agendas:
        print("   ⚠️ AUCUN AGENDA TROUVÉ - Vérifier la clé API")
        return []

    all_events = []
    agendas_with_events = 0

    for idx, agenda in enumerate(agendas):
        uid = agenda.get('uid')
        agenda_slug = agenda.get('slug')
        title = agenda.get('title', {})
        if isinstance(title, dict):
            agenda_title = title.get('fr') or title.get('en') or 'Agenda'
        else:
            agenda_title = title or 'Agenda'

        # Récupérer les événements de cet agenda avec filtrage géographique et temporel
        events_data = get_events_from_agenda(uid, center_lat, center_lon, radius_km, days_ahead, limit=100)
        events = events_data.get('events', []) if events_data else []

        if events:
            agendas_with_events += 1
            print(f"📖 [{idx+1}/{total_agendas}] {agenda_title}: {len(events)} événements")

        for ev in events:
            # Récupération du timing
            timings = ev.get('timings') or []
            begin_str = None
            end_str = None
            if timings:
                first_timing = timings[0]
                begin_str = first_timing.get('begin')
                end_str = first_timing.get('end')

            # Récupération de la localisation
            loc = ev.get('location') or {}
            ev_lat = loc.get('latitude')
            ev_lon = loc.get('longitude')

            # Si OpenAgenda ne fournit pas de lat/lon, on tente Nominatim
            if ev_lat is None or ev_lon is None:
                parts = []
                if loc.get("name"):
                    parts.append(str(loc["name"]))
                if loc.get("address"):
                    parts.append(str(loc["address"]))
                if loc.get("city"):
                    parts.append(str(loc["city"]))
                parts.append("France")
                address_str = ", ".join(parts)

                geocoded_lat, geocoded_lon = geocode_address_nominatim(address_str)
                if geocoded_lat is not None and geocoded_lon is not None:
                    ev_lat = geocoded_lat
                    ev_lon = geocoded_lon
                else:
                    continue

            try:
                ev_lat = float(ev_lat)
                ev_lon = float(ev_lon)
            except (ValueError, TypeError):
                continue

            # Calcul de la distance exacte
            dist = haversine_km(center_lat, center_lon, ev_lat, ev_lon)

            # Vérification finale du rayon
            if dist > radius_km:
                continue

            title_field = ev.get('title')
            if isinstance(title_field, dict):
                ev_title = title_field.get('fr') or title_field.get('en') or 'Événement'
            else:
                ev_title = title_field or 'Événement'

            # URL de l'événement
            event_slug = ev.get('slug')
            openagenda_url = None
            if agenda_slug and event_slug:
                openagenda_url = f"https://openagenda.com/{agenda_slug}/events/{event_slug}?lang=fr"

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

    print(f"✅ OpenAgenda: {len(all_events)} événements trouvés au total")
    print(f"   📊 {agendas_with_events}/{total_agendas} agendas avaient des événements dans la zone")
    return all_events


# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
def index():
    """Page d'accueil"""
    return send_from_directory('.', 'index.html')


@app.route('/api/events/nearby', methods=['GET'])
def get_nearby_events():
    """
    Récupère les événements à proximité d'une position
    Combine DATAtourisme (PostgreSQL) et OpenAgenda
    """
    
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        days_ahead = request.args.get('days', DAYS_AHEAD_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({
                "status": "error",
                "message": "Paramètres 'lat' et 'lon' requis"
            }), 400
        
        print(f"🔍 Recherche combinée: ({center_lat}, {center_lon}), rayon={radius_km}km, jours={days_ahead}")
        
        date_limite = datetime.now().date() + timedelta(days=days_ahead)
        
        all_events = []
        datatourisme_count = 0
        openagenda_count = 0
        
        # ========== 1. DATAtourisme (PostgreSQL) ==========
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            query = """
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
                    ST_Distance(
                        geom::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) / 1000 as "distanceKm"
                FROM evenements
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s
                )
                AND (date_debut IS NULL OR date_debut <= %s)
                AND (date_fin IS NULL OR date_fin >= CURRENT_DATE)
                ORDER BY "distanceKm", date_debut
                LIMIT 2000
            """
            
            cur.execute(query, (
                center_lon, center_lat,
                center_lon, center_lat,
                radius_km * 1000,
                date_limite
            ))
            
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
                event['agendaTitle'] = 'DATAtourisme National'
                
                contacts = event.get('contacts', '')
                event['openagendaUrl'] = ''
                if contacts and '#' in contacts:
                    parts = contacts.split('#')
                    for part in parts:
                        if part.startswith('http'):
                            event['openagendaUrl'] = part
                            break
                
                all_events.append(event)
            
            datatourisme_count = len(rows)
            cur.close()
            conn.close()
            
            print(f"✅ DATAtourisme: {datatourisme_count} événements trouvés")
            
        except psycopg2.Error as e:
            print(f"⚠️ Erreur PostgreSQL (DATAtourisme): {e}")
        
        # ========== 2. OpenAgenda ==========
        try:
            openagenda_events = fetch_openagenda_events(center_lat, center_lon, radius_km, days_ahead)
            openagenda_count = len(openagenda_events)
            all_events.extend(openagenda_events)
        except Exception as e:
            print(f"⚠️ Erreur OpenAgenda: {e}")
            import traceback
            traceback.print_exc()
        
        # ========== 3. Tri par distance puis date ==========
        all_events.sort(key=lambda e: (e.get("distanceKm") or 999, e.get("begin") or ""))
        
        print(f"✅ Total combiné: {len(all_events)} événements (DATAtourisme: {datatourisme_count}, OpenAgenda: {openagenda_count})")
        
        return jsonify({
            "status": "success",
            "center": {"latitude": center_lat, "longitude": center_lon},
            "radiusKm": radius_km,
            "days": days_ahead,
            "events": all_events,
            "count": len(all_events),
            "sources": {
                "DATAtourisme": datatourisme_count,
                "OpenAgenda": openagenda_count
            }
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": "Erreur interne du serveur",
            "details": str(e)
        }), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Retourne des statistiques sur la base"""
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) as total FROM evenements")
        total = cur.fetchone()['total']
        
        cur.execute("""
            SELECT COUNT(*) as count
            FROM evenements
            WHERE date_debut >= CURRENT_DATE
        """)
        futurs = cur.fetchone()['count']
        
        cur.execute("""
            SELECT commune, COUNT(*) as count
            FROM evenements
            WHERE commune IS NOT NULL
            GROUP BY commune
            ORDER BY count DESC
            LIMIT 10
        """)
        top_communes = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "total_events": total,
            "upcoming_events": futurs,
            "top_communes": [dict(row) for row in top_communes],
            "sources": ["DATAtourisme", "OpenAgenda"]
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health():
    """Endpoint de santé"""
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "sources": ["DATAtourisme PostgreSQL", "OpenAgenda", "Allociné" if ALLOCINE_AVAILABLE else "Allociné (non dispo)"]
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }), 500


# ============================================================================
# ALLOCINÉ - CINÉMAS ET SÉANCES
# ============================================================================

def fetch_allocine_cinemas(center_lat, center_lon, radius_km):
    """
    Récupère les séances de cinéma via allocine-seances.
    """
    if not ALLOCINE_AVAILABLE:
        print("⚠️ Allociné API non disponible")
        return []
    
    print(f"🎬 Allociné: Recherche autour de ({center_lat}, {center_lon}), rayon={radius_km}km")
    
    try:
        api = allocineAPI()
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Récupérer les top villes
        print("   🔍 Récupération des villes Allociné...")
        top_villes = api.get_top_villes()
        print(f"   📍 {len(top_villes)} villes disponibles")
        
        if not top_villes:
            print("   ❌ Aucune ville disponible")
            return []
        
        # Debug: afficher les premières villes
        print(f"   Premières villes: {[v.get('name') for v in top_villes[:5]]}")
        
        # Trouver la ville la plus proche par coordonnées
        # Mapping des grandes villes françaises avec leurs coordonnées
        villes_coords = {
            'paris': (48.8566, 2.3522),
            'marseille': (43.2965, 5.3698),
            'lyon': (45.7640, 4.8357),
            'toulouse': (43.6047, 1.4442),
            'nice': (43.7102, 7.2620),
            'nantes': (47.2184, -1.5536),
            'strasbourg': (48.5734, 7.7521),
            'montpellier': (43.6108, 3.8767),
            'bordeaux': (44.8378, -0.5792),
            'lille': (50.6292, 3.0573),
            'rennes': (48.1173, -1.6778),
            'reims': (49.2583, 4.0317),
            'le havre': (49.4944, 0.1079),
            'saint-etienne': (45.4397, 4.3872),
            'toulon': (43.1242, 5.9280),
            'grenoble': (45.1885, 5.7245),
            'dijon': (47.3220, 5.0415),
            'angers': (47.4784, -0.5632),
            'nimes': (43.8367, 4.3601),
            'villeurbanne': (45.7676, 4.8798),
            'clermont-ferrand': (45.7772, 3.0870),
            'aix-en-provence': (43.5297, 5.4474),
            'brest': (48.3904, -4.4861),
            'tours': (47.3941, 0.6848),
            'amiens': (49.8941, 2.2958),
            'limoges': (45.8336, 1.2611),
            'perpignan': (42.6986, 2.8954),
            'metz': (49.1193, 6.1757),
            'besancon': (47.2378, 6.0241),
            'orleans': (47.9029, 1.9093),
            'rouen': (49.4432, 1.0993),
            'caen': (49.1829, -0.3707),
            'nancy': (48.6921, 6.1844),
            'avignon': (43.9493, 4.8055),
            'cannes': (43.5528, 7.0174),
            'antibes': (43.5808, 7.1239),
        }
        
        # Trouver la ville Allociné la plus proche
        best_ville = None
        best_dist = float('inf')
        
        for ville in top_villes:
            ville_name = ville.get('name', '').lower()
            
            # Chercher dans notre mapping
            for nom, (vlat, vlon) in villes_coords.items():
                if nom in ville_name or ville_name in nom:
                    d = haversine_km(center_lat, center_lon, vlat, vlon)
                    if d < best_dist:
                        best_dist = d
                        best_ville = ville
                    break
        
        if not best_ville or best_dist > 100:
            # Prendre Paris par défaut si rien trouvé à moins de 100km
            for ville in top_villes:
                if 'paris' in ville.get('name', '').lower():
                    best_ville = ville
                    best_dist = haversine_km(center_lat, center_lon, 48.8566, 2.3522)
                    print(f"   ⚠️ Utilisation de Paris par défaut ({best_dist:.0f}km)")
                    break
        
        if not best_ville:
            print("   ❌ Aucune ville Allociné trouvée")
            return []
        
        location_id = best_ville.get('id')
        location_name = best_ville.get('name')
        print(f"   ✓ Ville sélectionnée: {location_name} (ID: {location_id}, {best_dist:.0f}km)")
        
        # Récupérer les cinémas
        cinemas = api.get_cinema(location_id)
        print(f"   🎥 {len(cinemas)} cinémas trouvés")
        
        if not cinemas:
            print("   ❌ Aucun cinéma trouvé")
            return []
        
        all_cinema_events = []
        cinemas_checked = 0
        
        for cinema in cinemas:
            cinema_name = cinema.get('name', 'Cinéma')
            cinema_address = cinema.get('address', '')
            cinema_id = cinema.get('id')
            
            # Pour les cinémas, on utilise le centre comme position approximative
            # (le géocodage prend trop de temps)
            cinema_lat = center_lat
            cinema_lon = center_lon
            dist = best_dist  # Distance approximative à la ville
            
            cinemas_checked += 1
            
            if cinemas_checked > 10:
                print(f"   ⚠️ Limite de 10 cinémas atteinte")
                break
            
            # Récupérer les films
            try:
                movies = api.get_movies(cinema_id, today)
                
                if movies:
                    print(f"   🎬 [{cinemas_checked}] {cinema_name}: {len(movies)} films")
                    for movie in movies:
                        film_title = movie.get('title', 'Film inconnu')
                        
                        all_cinema_events.append({
                            "uid": f"allocine-{cinema_id}-{movie.get('id', '')}",
                            "title": f"🎬 {film_title}",
                            "begin": today,
                            "end": today,
                            "locationName": cinema_name,
                            "city": location_name,
                            "address": cinema_address,
                            "latitude": cinema_lat,
                            "longitude": cinema_lon,
                            "distanceKm": round(dist, 1),
                            "openagendaUrl": "",
                            "agendaTitle": cinema_name,
                            "source": "Allocine",
                            "director": movie.get('director', ''),
                            "genres": movie.get('genres', []),
                            "runtime": movie.get('runtime', 0),
                            "poster": movie.get('urlPoster', ''),
                            "synopsis": movie.get('synopsisFull', '')[:200] if movie.get('synopsisFull') else ''
                        })
                else:
                    print(f"   🎬 [{cinemas_checked}] {cinema_name}: aucun film")
                        
            except Exception as e:
                print(f"      ⚠️ Erreur films pour {cinema_name}: {e}")
                continue
        
        print(f"✅ Allociné: {len(all_cinema_events)} séances trouvées")
        return all_cinema_events
        
    except Exception as e:
        print(f"❌ Erreur Allociné: {e}")
        import traceback
        traceback.print_exc()
        return []


@app.route('/api/cinema/nearby', methods=['GET'])
def get_nearby_cinema():
    """
    Récupère les séances de cinéma à proximité d'une position
    """
    try:
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({
                "status": "error",
                "message": "Paramètres 'lat' et 'lon' requis"
            }), 400
        
        cinema_events = fetch_allocine_cinemas(center_lat, center_lon, radius_km)
        
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
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# ============================================================================
# LANCEMENT DU SERVEUR
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    
    print("="*70)
    print("🚀 API DATATOURISME + OPENAGENDA + ALLOCINÉ")
    print("="*70)
    print(f"Port: {port}")
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"OpenAgenda API: {BASE_URL}")
    print(f"Allociné: {'Disponible' if ALLOCINE_AVAILABLE else 'Non disponible'}")
    print(f"Rayon par défaut: {RADIUS_KM_DEFAULT} km")
    print(f"Période par défaut: {DAYS_AHEAD_DEFAULT} jours")
    print("="*70)
    print()
    
    app.run(host='0.0.0.0', port=port, debug=True)
