#!/usr/bin/env python3
"""
API Flask pour servir les événements DATAtourisme depuis PostgreSQL
Alternative nationale à OpenAgenda
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

# PostgreSQL - Support pour Render et local
database_url = os.environ.get('DATABASE_URL')

if database_url:
    # Render fournit DATABASE_URL
    url = urlparse(database_url)
    DB_CONFIG = {
        'host': url.hostname,
        'port': url.port or 5432,
        'database': url.path[1:],  # Enlever le / initial
        'user': url.username,
        'password': url.password,
        'sslmode': 'require'  # Important pour Render
    }
    print(f"✅ Connexion à Render: {url.hostname}")
else:
    # Configuration locale pour développement
    DB_CONFIG = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': int(os.environ.get('DB_PORT', 5432)),
        'database': os.environ.get('DB_NAME', 'datatourisme'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'sslmode': 'prefer'
    }
    print(f"⚠️  Connexion locale: {DB_CONFIG['host']}")

# Valeurs par défaut
RADIUS_KM_DEFAULT = 30
DAYS_AHEAD_DEFAULT = 30


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def get_db_connection():
    """Crée une connexion à PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


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
    
    Paramètres:
        - lat (float, requis): Latitude du centre
        - lon (float, requis): Longitude du centre
        - radiusKm (int, optionnel): Rayon en km (défaut: 30)
        - days (int, optionnel): Nombre de jours (défaut: 30)
    """
    
    try:
        # Récupération des paramètres
        center_lat = request.args.get('lat', type=float)
        center_lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radiusKm', RADIUS_KM_DEFAULT, type=int)
        days_ahead = request.args.get('days', DAYS_AHEAD_DEFAULT, type=int)
        
        if center_lat is None or center_lon is None:
            return jsonify({
                "status": "error",
                "message": "Paramètres 'lat' et 'lon' requis"
            }), 400
        
        print(f"🔍 Recherche: ({center_lat}, {center_lon}), rayon={radius_km}km, jours={days_ahead}")
        
        # Date limite
        date_limite = datetime.now().date() + timedelta(days=days_ahead)
        
        # Connexion PostgreSQL
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Requête avec PostGIS
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
                %s  -- rayon en mètres
            )
            AND (date_debut IS NULL OR date_debut <= %s)
            AND (date_fin IS NULL OR date_fin >= CURRENT_DATE)
            ORDER BY "distanceKm", date_debut
            LIMIT 500
        """
        
        cur.execute(query, (
            center_lon, center_lat,  # Pour le calcul de distance
            center_lon, center_lat,  # Pour le filtre géographique
            radius_km * 1000,        # Rayon en mètres
            date_limite
        ))
        
        rows = cur.fetchall()
        
        # Formater les résultats
        events = []
        for row in rows:
            event = dict(row)
            
            # Convertir les dates en ISO string
            if event.get('begin'):
                event['begin'] = event['begin'].isoformat()
            if event.get('end'):
                event['end'] = event['end'].isoformat()
            
            # Arrondir la distance
            if event.get('distanceKm'):
                event['distanceKm'] = round(event['distanceKm'], 1)
            
            # Ajouter locationName
            event['locationName'] = event.get('city', '')
            
            # Ajouter source
            event['source'] = 'DATAtourisme'
            event['agendaTitle'] = 'DATAtourisme National'
            
            # Parser les contacts pour extraire l'URL
            contacts = event.get('contacts', '')
            event['openagendaUrl'] = ''
            if contacts and '#' in contacts:
                parts = contacts.split('#')
                for part in parts:
                    if part.startswith('http'):
                        event['openagendaUrl'] = part
                        break
            
            events.append(event)
        
        cur.close()
        conn.close()
        
        print(f"✅ {len(events)} événements trouvés")
        
        return jsonify({
            "status": "success",
            "center": {"latitude": center_lat, "longitude": center_lon},
            "radiusKm": radius_km,
            "days": days_ahead,
            "events": events,
            "count": len(events),
            "source": "DATAtourisme PostgreSQL"
        }), 200
        
    except psycopg2.Error as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        return jsonify({
            "status": "error",
            "message": "Erreur de base de données",
            "details": str(e)
        }), 500
    
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
        
        # Total événements
        cur.execute("SELECT COUNT(*) as total FROM evenements")
        total = cur.fetchone()['total']
        
        # Événements à venir
        cur.execute("""
            SELECT COUNT(*) as count
            FROM evenements
            WHERE date_debut >= CURRENT_DATE
        """)
        futurs = cur.fetchone()['count']
        
        # Top communes
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
            "top_communes": [dict(row) for row in top_communes]
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health():
    """Endpoint de santé pour vérifier que l'API fonctionne"""
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "source": "DATAtourisme PostgreSQL"
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }), 500


# ============================================================================
# LANCEMENT DU SERVEUR
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    
    print("="*70)
    print("🚀 API DATATOURISME + POSTGRESQL")
    print("="*70)
    print(f"Port: {port}")
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"Rayon par défaut: {RADIUS_KM_DEFAULT} km")
    print(f"Période par défaut: {DAYS_AHEAD_DEFAULT} jours")
    print("="*70)
    print()
    
    app.run(host='0.0.0.0', port=port, debug=True)
