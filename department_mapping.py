#!/usr/bin/env python3
"""
🗺️ MAPPING DÉPARTEMENTS : Nominatim ↔ Allociné

Ce fichier contient le mapping statique entre :
- Les noms retournés par Nominatim (reverse geocoding)
- Les IDs utilisés par l'API Allociné

Utilisation :
    from department_mapping import get_allocine_dept_id, get_all_dept_ids_for_location
"""

# ============================================================================
# MAPPING PRINCIPAL : Nom Nominatim → ID Allociné
# ============================================================================
# 
# Format des IDs Allociné : "departement-XXXXX"
# Les IDs ont été extraits de l'API Allociné via get_departements()
#
# Note : Nominatim peut renvoyer différentes variantes selon la localisation :
# - Le nom du département (ex: "Haute-Garonne")
# - Le nom de la région (ex: "Occitanie") 
# - Le nom de la ville pour les grandes métropoles (ex: "Paris", "Lyon")

NOMINATIM_TO_ALLOCINE = {
    # =========================================================================
    # DÉPARTEMENTS MÉTROPOLITAINS (01-95)
    # =========================================================================
    
    # 01 - Ain
    "ain": "departement-83191",
    
    # 02 - Aisne
    "aisne": "departement-83178",
    
    # 03 - Allier
    "allier": "departement-83111",
    
    # 04 - Alpes-de-Haute-Provence
    "alpes-de-haute-provence": "departement-83185",
    "alpes de haute-provence": "departement-83185",
    "haute-provence": "departement-83185",
    
    # 05 - Hautes-Alpes
    "hautes-alpes": "departement-83186",
    "hautes alpes": "departement-83186",
    
    # 06 - Alpes-Maritimes
    "alpes-maritimes": "departement-83187",
    "alpes maritimes": "departement-83187",
    
    # 07 - Ardèche
    "ardèche": "departement-83112",
    "ardeche": "departement-83112",
    
    # 08 - Ardennes
    "ardennes": "departement-83179",
    
    # 09 - Ariège
    "ariège": "departement-83188",
    "ariege": "departement-83188",
    
    # 10 - Aube
    "aube": "departement-83113",
    
    # 11 - Aude
    "aude": "departement-83189",
    
    # 12 - Aveyron
    "aveyron": "departement-83190",
    
    # 13 - Bouches-du-Rhône
    "bouches-du-rhône": "departement-83192",
    "bouches-du-rhone": "departement-83192",
    "bouches du rhône": "departement-83192",
    "bouches du rhone": "departement-83192",
    
    # 14 - Calvados
    "calvados": "departement-83146",
    
    # 15 - Cantal
    "cantal": "departement-83114",
    
    # 16 - Charente
    "charente": "departement-83156",
    
    # 17 - Charente-Maritime
    "charente-maritime": "departement-83157",
    "charente maritime": "departement-83157",
    
    # 18 - Cher
    "cher": "departement-83117",
    
    # 19 - Corrèze
    "corrèze": "departement-83135",
    "correze": "departement-83135",
    
    # 21 - Côte-d'Or
    "côte-d'or": "departement-83115",
    "cote-d'or": "departement-83115",
    "côte d'or": "departement-83115",
    "cote d'or": "departement-83115",
    
    # 22 - Côtes-d'Armor
    "côtes-d'armor": "departement-83116",
    "cotes-d'armor": "departement-83116",
    "côtes d'armor": "departement-83116",
    "cotes d'armor": "departement-83116",
    
    # 23 - Creuse
    "creuse": "departement-83136",
    
    # 24 - Dordogne
    "dordogne": "departement-83103",
    
    # 25 - Doubs
    "doubs": "departement-83126",
    
    # 26 - Drôme
    "drôme": "departement-83193",
    "drome": "departement-83193",
    
    # 27 - Eure
    "eure": "departement-83130",
    
    # 28 - Eure-et-Loir
    "eure-et-loir": "departement-83118",
    "eure et loir": "departement-83118",
    
    # 29 - Finistère
    "finistère": "departement-83116",
    "finistere": "departement-83116",
    
    # 30 - Gard
    "gard": "departement-83194",
    
    # 31 - Haute-Garonne
    "haute-garonne": "departement-83195",
    "haute garonne": "departement-83195",
    
    # 32 - Gers
    "gers": "departement-83196",
    
    # 33 - Gironde
    "gironde": "departement-83104",
    
    # 34 - Hérault
    "hérault": "departement-83197",
    "herault": "departement-83197",
    
    # 35 - Ille-et-Vilaine
    "ille-et-vilaine": "departement-83116",
    "ille et vilaine": "departement-83116",
    
    # 36 - Indre
    "indre": "departement-83119",
    
    # 37 - Indre-et-Loire
    "indre-et-loire": "departement-83120",
    "indre et loire": "departement-83120",
    
    # 38 - Isère
    "isère": "departement-83198",
    "isere": "departement-83198",
    
    # 39 - Jura
    "jura": "departement-83127",
    
    # 40 - Landes
    "landes": "departement-83105",
    
    # 41 - Loir-et-Cher
    "loir-et-cher": "departement-83121",
    "loir et cher": "departement-83121",
    
    # 42 - Loire
    "loire": "departement-83199",
    
    # 43 - Haute-Loire
    "haute-loire": "departement-83200",
    "haute loire": "departement-83200",
    
    # 44 - Loire-Atlantique
    "loire-atlantique": "departement-83153",
    "loire atlantique": "departement-83153",
    
    # 45 - Loiret
    "loiret": "departement-83122",
    
    # 46 - Lot
    "lot": "departement-83201",
    
    # 47 - Lot-et-Garonne
    "lot-et-garonne": "departement-83106",
    "lot et garonne": "departement-83106",
    
    # 48 - Lozère
    "lozère": "departement-83202",
    "lozere": "departement-83202",
    
    # 49 - Maine-et-Loire
    "maine-et-loire": "departement-83154",
    "maine et loire": "departement-83154",
    
    # 50 - Manche
    "manche": "departement-83147",
    
    # 51 - Marne
    "marne": "departement-83180",
    
    # 52 - Haute-Marne
    "haute-marne": "departement-83181",
    "haute marne": "departement-83181",
    
    # 53 - Mayenne
    "mayenne": "departement-83155",
    
    # 54 - Meurthe-et-Moselle
    "meurthe-et-moselle": "departement-83138",
    "meurthe et moselle": "departement-83138",
    
    # 55 - Meuse
    "meuse": "departement-83139",
    
    # 56 - Morbihan
    "morbihan": "departement-83116",
    
    # 57 - Moselle
    "moselle": "departement-83140",
    
    # 58 - Nièvre
    "nièvre": "departement-83123",
    "nievre": "departement-83123",
    
    # 59 - Nord
    "nord": "departement-83144",
    
    # 60 - Oise
    "oise": "departement-83167",
    
    # 61 - Orne
    "orne": "departement-83148",
    
    # 62 - Pas-de-Calais
    "pas-de-calais": "departement-83145",
    "pas de calais": "departement-83145",
    
    # 63 - Puy-de-Dôme
    "puy-de-dôme": "departement-83124",
    "puy-de-dome": "departement-83124",
    "puy de dôme": "departement-83124",
    "puy de dome": "departement-83124",
    
    # 64 - Pyrénées-Atlantiques
    "pyrénées-atlantiques": "departement-83107",
    "pyrenees-atlantiques": "departement-83107",
    "pyrénées atlantiques": "departement-83107",
    "pyrenees atlantiques": "departement-83107",
    
    # 65 - Hautes-Pyrénées
    "hautes-pyrénées": "departement-83203",
    "hautes-pyrenees": "departement-83203",
    "hautes pyrénées": "departement-83203",
    "hautes pyrenees": "departement-83203",
    
    # 66 - Pyrénées-Orientales
    "pyrénées-orientales": "departement-83204",
    "pyrenees-orientales": "departement-83204",
    "pyrénées orientales": "departement-83204",
    "pyrenees orientales": "departement-83204",
    
    # 67 - Bas-Rhin
    "bas-rhin": "departement-83109",
    "bas rhin": "departement-83109",
    
    # 68 - Haut-Rhin
    "haut-rhin": "departement-83110",
    "haut rhin": "departement-83110",
    
    # 69 - Rhône
    "rhône": "departement-83205",
    "rhone": "departement-83205",
    
    # 70 - Haute-Saône
    "haute-saône": "departement-83128",
    "haute-saone": "departement-83128",
    "haute saône": "departement-83128",
    "haute saone": "departement-83128",
    
    # 71 - Saône-et-Loire
    "saône-et-loire": "departement-83125",
    "saone-et-loire": "departement-83125",
    "saône et loire": "departement-83125",
    "saone et loire": "departement-83125",
    
    # 72 - Sarthe
    "sarthe": "departement-83155",
    
    # 73 - Savoie
    "savoie": "departement-83206",
    
    # 74 - Haute-Savoie
    "haute-savoie": "departement-83207",
    "haute savoie": "departement-83207",
    
    # 75 - Paris
    "paris": "departement-83168",
    
    # 76 - Seine-Maritime
    "seine-maritime": "departement-83131",
    "seine maritime": "departement-83131",
    
    # 77 - Seine-et-Marne
    "seine-et-marne": "departement-83169",
    "seine et marne": "departement-83169",
    
    # 78 - Yvelines
    "yvelines": "departement-83170",
    
    # 79 - Deux-Sèvres
    "deux-sèvres": "departement-83158",
    "deux-sevres": "departement-83158",
    "deux sèvres": "departement-83158",
    "deux sevres": "departement-83158",
    
    # 80 - Somme
    "somme": "departement-83182",
    
    # 81 - Tarn
    "tarn": "departement-83208",
    
    # 82 - Tarn-et-Garonne
    "tarn-et-garonne": "departement-83209",
    "tarn et garonne": "departement-83209",
    
    # 83 - Var
    "var": "departement-83210",
    
    # 84 - Vaucluse
    "vaucluse": "departement-83211",
    
    # 85 - Vendée
    "vendée": "departement-83159",
    "vendee": "departement-83159",
    
    # 86 - Vienne
    "vienne": "departement-83160",
    
    # 87 - Haute-Vienne
    "haute-vienne": "departement-83137",
    "haute vienne": "departement-83137",
    
    # 88 - Vosges
    "vosges": "departement-83141",
    
    # 89 - Yonne
    "yonne": "departement-83126",
    
    # 90 - Territoire de Belfort
    "territoire de belfort": "departement-83129",
    "belfort": "departement-83129",
    
    # 91 - Essonne
    "essonne": "departement-83171",
    
    # 92 - Hauts-de-Seine
    "hauts-de-seine": "departement-83172",
    "hauts de seine": "departement-83172",
    
    # 93 - Seine-Saint-Denis
    "seine-saint-denis": "departement-83173",
    "seine saint denis": "departement-83173",
    
    # 94 - Val-de-Marne
    "val-de-marne": "departement-83174",
    "val de marne": "departement-83174",
    
    # 95 - Val-d'Oise
    "val-d'oise": "departement-83175",
    "val d'oise": "departement-83175",
    "val-doise": "departement-83175",
    "val doise": "departement-83175",
    
    # =========================================================================
    # CORSE (2A, 2B)
    # =========================================================================
    
    # 2A - Corse-du-Sud
    "corse-du-sud": "departement-83183",
    "corse du sud": "departement-83183",
    
    # 2B - Haute-Corse
    "haute-corse": "departement-83184",
    "haute corse": "departement-83184",
    
    # Corse (générique)
    "corse": "departement-83183",
    
    # =========================================================================
    # DOM-TOM (si disponibles sur Allociné)
    # =========================================================================
    
    # 971 - Guadeloupe
    "guadeloupe": "departement-83212",
    
    # 972 - Martinique
    "martinique": "departement-83213",
    
    # 973 - Guyane
    "guyane": "departement-83214",
    "guyane française": "departement-83214",
    
    # 974 - La Réunion
    "la réunion": "departement-83215",
    "la reunion": "departement-83215",
    "réunion": "departement-83215",
    "reunion": "departement-83215",
    
    # 976 - Mayotte
    "mayotte": "departement-83216",
    
    # =========================================================================
    # GRANDES VILLES (Nominatim peut renvoyer le nom de la ville)
    # =========================================================================
    
    "lyon": "departement-83205",           # → Rhône
    "marseille": "departement-83192",      # → Bouches-du-Rhône
    "toulouse": "departement-83195",       # → Haute-Garonne
    "nice": "departement-83187",           # → Alpes-Maritimes
    "nantes": "departement-83153",         # → Loire-Atlantique
    "strasbourg": "departement-83109",     # → Bas-Rhin
    "montpellier": "departement-83197",    # → Hérault
    "bordeaux": "departement-83104",       # → Gironde
    "lille": "departement-83144",          # → Nord
    "rennes": "departement-83116",         # → Ille-et-Vilaine
    "reims": "departement-83180",          # → Marne
    "le havre": "departement-83131",       # → Seine-Maritime
    "saint-étienne": "departement-83199",  # → Loire
    "saint-etienne": "departement-83199",
    "toulon": "departement-83210",         # → Var
    "grenoble": "departement-83198",       # → Isère
    "dijon": "departement-83115",          # → Côte-d'Or
    "angers": "departement-83154",         # → Maine-et-Loire
    "nîmes": "departement-83194",          # → Gard
    "nimes": "departement-83194",
    "villeurbanne": "departement-83205",   # → Rhône
    "clermont-ferrand": "departement-83124", # → Puy-de-Dôme
    "le mans": "departement-83155",        # → Sarthe
    "aix-en-provence": "departement-83192", # → Bouches-du-Rhône
    "brest": "departement-83116",          # → Finistère
    "tours": "departement-83120",          # → Indre-et-Loire
    "amiens": "departement-83182",         # → Somme
    "limoges": "departement-83137",        # → Haute-Vienne
    "perpignan": "departement-83204",      # → Pyrénées-Orientales
    "besançon": "departement-83126",       # → Doubs
    "besancon": "departement-83126",
    "orléans": "departement-83122",        # → Loiret
    "orleans": "departement-83122",
    "rouen": "departement-83131",          # → Seine-Maritime
    "mulhouse": "departement-83110",       # → Haut-Rhin
    "caen": "departement-83146",           # → Calvados
    "nancy": "departement-83138",          # → Meurthe-et-Moselle
    "metz": "departement-83140",           # → Moselle
    "argenteuil": "departement-83175",     # → Val-d'Oise
    "montreuil": "departement-83173",      # → Seine-Saint-Denis
    "saint-denis": "departement-83173",    # → Seine-Saint-Denis
    "roubaix": "departement-83144",        # → Nord
    "tourcoing": "departement-83144",      # → Nord
    "avignon": "departement-83211",        # → Vaucluse
    "poitiers": "departement-83160",       # → Vienne
    "cannes": "departement-83187",         # → Alpes-Maritimes
    "antibes": "departement-83187",        # → Alpes-Maritimes
    
    # =========================================================================
    # RÉGIONS (Nominatim peut renvoyer la région au lieu du département)
    # → On renvoie le département principal / plus peuplé
    # =========================================================================
    
    "île-de-france": "departement-83168",      # → Paris
    "ile-de-france": "departement-83168",
    "île de france": "departement-83168",
    "ile de france": "departement-83168",
    "idf": "departement-83168",
    
    "auvergne-rhône-alpes": "departement-83205",   # → Rhône
    "auvergne-rhone-alpes": "departement-83205",
    "rhône-alpes": "departement-83205",
    "rhone-alpes": "departement-83205",
    
    "nouvelle-aquitaine": "departement-83104",     # → Gironde
    "aquitaine": "departement-83104",
    
    "occitanie": "departement-83195",              # → Haute-Garonne
    "midi-pyrénées": "departement-83195",
    "midi-pyrenees": "departement-83195",
    "languedoc-roussillon": "departement-83197",   # → Hérault
    
    "grand est": "departement-83109",              # → Bas-Rhin
    "alsace": "departement-83109",
    "lorraine": "departement-83140",               # → Moselle
    "champagne-ardenne": "departement-83180",      # → Marne
    
    "hauts-de-france": "departement-83144",        # → Nord
    "hauts de france": "departement-83144",
    "nord-pas-de-calais": "departement-83144",
    "picardie": "departement-83182",               # → Somme
    
    "normandie": "departement-83131",              # → Seine-Maritime
    "haute-normandie": "departement-83131",
    "basse-normandie": "departement-83146",        # → Calvados
    
    "bretagne": "departement-83116",               # → Ille-et-Vilaine (ou Finistère)
    
    "pays de la loire": "departement-83153",       # → Loire-Atlantique
    "pays-de-la-loire": "departement-83153",
    
    "centre-val de loire": "departement-83122",    # → Loiret
    "centre": "departement-83122",
    
    "bourgogne-franche-comté": "departement-83115", # → Côte-d'Or
    "bourgogne-franche-comte": "departement-83115",
    "bourgogne": "departement-83115",
    "franche-comté": "departement-83126",          # → Doubs
    "franche-comte": "departement-83126",
    
    "provence-alpes-côte d'azur": "departement-83192",  # → Bouches-du-Rhône
    "provence-alpes-cote d'azur": "departement-83192",
    "paca": "departement-83192",
    "provence": "departement-83192",
    "côte d'azur": "departement-83187",            # → Alpes-Maritimes
    "cote d'azur": "departement-83187",
}


# ============================================================================
# MAPPING INVERSE : Code postal → ID Allociné (plus fiable)
# ============================================================================

POSTCODE_TO_ALLOCINE = {
    "01": "departement-83191",  # Ain
    "02": "departement-83178",  # Aisne
    "03": "departement-83111",  # Allier
    "04": "departement-83185",  # Alpes-de-Haute-Provence
    "05": "departement-83186",  # Hautes-Alpes
    "06": "departement-83187",  # Alpes-Maritimes
    "07": "departement-83112",  # Ardèche
    "08": "departement-83179",  # Ardennes
    "09": "departement-83188",  # Ariège
    "10": "departement-83113",  # Aube
    "11": "departement-83189",  # Aude
    "12": "departement-83190",  # Aveyron
    "13": "departement-83192",  # Bouches-du-Rhône
    "14": "departement-83146",  # Calvados
    "15": "departement-83114",  # Cantal
    "16": "departement-83156",  # Charente
    "17": "departement-83157",  # Charente-Maritime
    "18": "departement-83117",  # Cher
    "19": "departement-83135",  # Corrèze
    "20": "departement-83183",  # Corse (2A par défaut)
    "2A": "departement-83183",  # Corse-du-Sud
    "2B": "departement-83184",  # Haute-Corse
    "21": "departement-83115",  # Côte-d'Or
    "22": "departement-83116",  # Côtes-d'Armor
    "23": "departement-83136",  # Creuse
    "24": "departement-83103",  # Dordogne
    "25": "departement-83126",  # Doubs
    "26": "departement-83193",  # Drôme
    "27": "departement-83130",  # Eure
    "28": "departement-83118",  # Eure-et-Loir
    "29": "departement-83116",  # Finistère
    "30": "departement-83194",  # Gard
    "31": "departement-83195",  # Haute-Garonne
    "32": "departement-83196",  # Gers
    "33": "departement-83104",  # Gironde
    "34": "departement-83197",  # Hérault
    "35": "departement-83116",  # Ille-et-Vilaine
    "36": "departement-83119",  # Indre
    "37": "departement-83120",  # Indre-et-Loire
    "38": "departement-83198",  # Isère
    "39": "departement-83127",  # Jura
    "40": "departement-83105",  # Landes
    "41": "departement-83121",  # Loir-et-Cher
    "42": "departement-83199",  # Loire
    "43": "departement-83200",  # Haute-Loire
    "44": "departement-83153",  # Loire-Atlantique
    "45": "departement-83122",  # Loiret
    "46": "departement-83201",  # Lot
    "47": "departement-83106",  # Lot-et-Garonne
    "48": "departement-83202",  # Lozère
    "49": "departement-83154",  # Maine-et-Loire
    "50": "departement-83147",  # Manche
    "51": "departement-83180",  # Marne
    "52": "departement-83181",  # Haute-Marne
    "53": "departement-83155",  # Mayenne
    "54": "departement-83138",  # Meurthe-et-Moselle
    "55": "departement-83139",  # Meuse
    "56": "departement-83116",  # Morbihan
    "57": "departement-83140",  # Moselle
    "58": "departement-83123",  # Nièvre
    "59": "departement-83144",  # Nord
    "60": "departement-83167",  # Oise
    "61": "departement-83148",  # Orne
    "62": "departement-83145",  # Pas-de-Calais
    "63": "departement-83124",  # Puy-de-Dôme
    "64": "departement-83107",  # Pyrénées-Atlantiques
    "65": "departement-83203",  # Hautes-Pyrénées
    "66": "departement-83204",  # Pyrénées-Orientales
    "67": "departement-83109",  # Bas-Rhin
    "68": "departement-83110",  # Haut-Rhin
    "69": "departement-83205",  # Rhône
    "70": "departement-83128",  # Haute-Saône
    "71": "departement-83125",  # Saône-et-Loire
    "72": "departement-83155",  # Sarthe
    "73": "departement-83206",  # Savoie
    "74": "departement-83207",  # Haute-Savoie
    "75": "departement-83168",  # Paris
    "76": "departement-83131",  # Seine-Maritime
    "77": "departement-83169",  # Seine-et-Marne
    "78": "departement-83170",  # Yvelines
    "79": "departement-83158",  # Deux-Sèvres
    "80": "departement-83182",  # Somme
    "81": "departement-83208",  # Tarn
    "82": "departement-83209",  # Tarn-et-Garonne
    "83": "departement-83210",  # Var
    "84": "departement-83211",  # Vaucluse
    "85": "departement-83159",  # Vendée
    "86": "departement-83160",  # Vienne
    "87": "departement-83137",  # Haute-Vienne
    "88": "departement-83141",  # Vosges
    "89": "departement-83126",  # Yonne
    "90": "departement-83129",  # Territoire de Belfort
    "91": "departement-83171",  # Essonne
    "92": "departement-83172",  # Hauts-de-Seine
    "93": "departement-83173",  # Seine-Saint-Denis
    "94": "departement-83174",  # Val-de-Marne
    "95": "departement-83175",  # Val-d'Oise
    # DOM-TOM
    "971": "departement-83212", # Guadeloupe
    "972": "departement-83213", # Martinique
    "973": "departement-83214", # Guyane
    "974": "departement-83215", # La Réunion
    "976": "departement-83216", # Mayotte
}


# ============================================================================
# ÎLE-DE-FRANCE : Mapping spécial (recherche dans plusieurs départements)
# ============================================================================

IDF_DEPARTMENTS = [
    "departement-83168",  # 75 - Paris
    "departement-83172",  # 92 - Hauts-de-Seine
    "departement-83173",  # 93 - Seine-Saint-Denis
    "departement-83174",  # 94 - Val-de-Marne
    "departement-83169",  # 77 - Seine-et-Marne
    "departement-83170",  # 78 - Yvelines
    "departement-83171",  # 91 - Essonne
    "departement-83175",  # 95 - Val-d'Oise
]


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def normalize_name(name):
    """Normalise un nom pour la recherche (minuscules, sans accents superflus)."""
    if not name:
        return ""
    return name.lower().strip()


def get_allocine_dept_id(nominatim_name):
    """
    Retourne l'ID Allociné pour un nom de département/ville/région Nominatim.
    
    Args:
        nominatim_name: Nom retourné par Nominatim (ex: "Haute-Garonne", "Paris", "Occitanie")
    
    Returns:
        ID Allociné (ex: "departement-83195") ou None si non trouvé
    """
    if not nominatim_name:
        return None
    
    normalized = normalize_name(nominatim_name)
    
    # Recherche exacte
    if normalized in NOMINATIM_TO_ALLOCINE:
        return NOMINATIM_TO_ALLOCINE[normalized]
    
    # Recherche partielle (le nom Nominatim contient le nom du mapping)
    for key, value in NOMINATIM_TO_ALLOCINE.items():
        if key in normalized or normalized in key:
            return value
    
    return None


def get_allocine_dept_id_from_postcode(postcode):
    """
    Retourne l'ID Allociné à partir d'un code postal.
    
    Args:
        postcode: Code postal (ex: "75001", "31000", "2A004")
    
    Returns:
        ID Allociné ou None
    """
    if not postcode:
        return None
    
    postcode = str(postcode).strip()
    
    # Cas Corse
    if postcode.upper().startswith("2A"):
        return POSTCODE_TO_ALLOCINE.get("2A")
    if postcode.upper().startswith("2B"):
        return POSTCODE_TO_ALLOCINE.get("2B")
    
    # DOM-TOM (3 chiffres)
    if len(postcode) >= 3 and postcode[:3] in POSTCODE_TO_ALLOCINE:
        return POSTCODE_TO_ALLOCINE.get(postcode[:3])
    
    # Métropole (2 premiers chiffres)
    if len(postcode) >= 2:
        dept_code = postcode[:2]
        return POSTCODE_TO_ALLOCINE.get(dept_code)
    
    return None


def get_all_dept_ids_for_location(nominatim_name, postcode=None):
    """
    Retourne une liste d'IDs Allociné pour une localisation.
    Pour l'Île-de-France, retourne tous les départements IDF.
    
    Args:
        nominatim_name: Nom de la localisation
        postcode: Code postal (optionnel, plus fiable)
    
    Returns:
        Liste d'IDs Allociné
    """
    # Si code postal disponible, l'utiliser en priorité
    if postcode:
        dept_id = get_allocine_dept_id_from_postcode(postcode)
        if dept_id:
            # Vérifier si c'est en IDF
            if dept_id in IDF_DEPARTMENTS:
                return IDF_DEPARTMENTS
            return [dept_id]
    
    # Sinon, utiliser le nom
    if nominatim_name:
        normalized = normalize_name(nominatim_name)
        
        # Cas spécial : Île-de-France ou Paris
        if any(x in normalized for x in ["île-de-france", "ile-de-france", "paris", "idf"]):
            return IDF_DEPARTMENTS
        
        dept_id = get_allocine_dept_id(nominatim_name)
        if dept_id:
            # Vérifier si c'est en IDF
            if dept_id in IDF_DEPARTMENTS:
                return IDF_DEPARTMENTS
            return [dept_id]
    
    return []


def is_in_idf(nominatim_name=None, postcode=None):
    """Vérifie si une localisation est en Île-de-France."""
    if postcode:
        dept_code = str(postcode)[:2]
        return dept_code in ["75", "77", "78", "91", "92", "93", "94", "95"]
    
    if nominatim_name:
        normalized = normalize_name(nominatim_name)
        idf_keywords = ["île-de-france", "ile-de-france", "paris", "hauts-de-seine", 
                        "seine-saint-denis", "val-de-marne", "seine-et-marne",
                        "yvelines", "essonne", "val-d'oise", "val d'oise"]
        return any(kw in normalized for kw in idf_keywords)
    
    return False


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    # Tests
    test_cases = [
        "Haute-Garonne",
        "Paris",
        "Occitanie",
        "Lyon",
        "Rhône",
        "Île-de-France",
        "Bouches-du-Rhône",
        "marseille",
        "NORD",
        "Alpes-Maritimes",
        "Nice",
        "Toulouse",
    ]
    
    print("=" * 60)
    print("TEST DU MAPPING NOMINATIM → ALLOCINÉ")
    print("=" * 60)
    
    for name in test_cases:
        result = get_allocine_dept_id(name)
        print(f"{name:25} → {result}")
    
    print()
    print("=" * 60)
    print("TEST DU MAPPING CODE POSTAL → ALLOCINÉ")
    print("=" * 60)
    
    postcodes = ["75001", "31000", "69001", "13001", "59000", "2A004", "97100"]
    for pc in postcodes:
        result = get_allocine_dept_id_from_postcode(pc)
        print(f"{pc:10} → {result}")
    
    print()
    print("=" * 60)
    print("TEST ÎLE-DE-FRANCE")
    print("=" * 60)
    
    print(f"Paris est en IDF: {is_in_idf('Paris')}")
    print(f"Lyon est en IDF: {is_in_idf('Lyon')}")
    print(f"Code 75001 est en IDF: {is_in_idf(postcode='75001')}")
    print(f"Code 31000 est en IDF: {is_in_idf(postcode='31000')}")
    
    print()
    print(f"Départements pour Paris: {get_all_dept_ids_for_location('Paris')}")
    print(f"Départements pour Lyon: {get_all_dept_ids_for_location('Lyon')}")
