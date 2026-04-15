#!/usr/bin/env python3
"""
Scraping automatique des fiches DMC depuis l'annuaire DestiMaG / TourMaG.
Ce script :
1. Parcourt la page annuaire pour lister toutes les fiches DMC
2. Scrape chaque fiche pour extraire les données structurées
3. Génère un fichier dmc_data.json
"""

import json
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# =============================================================================
# CONFIGURATION
# =============================================================================

ANNUAIRE_URL = "https://www.tourmag.com/Annuaire-des-agences-touristiques-locales_r404.html"
BASE_URL = "https://www.tourmag.com"
OUTPUT_FILE = "data/dmc_data.json"
REQUEST_DELAY = 1.5
USER_AGENT = "Mozilla/5.0 (compatible; DMCMap-Scraper/1.0; TourMaG)"

# URLs d'articles d'actualité connus qui se mélangent dans l'annuaire
NEWS_PATTERNS = [
    "Action-Visas", "Actualites-dans", "Chine-l-Annee", "Cyclisme-F1",
    "DITEX-2026", "Distribution-la-guerre", "Evantia-Giumba", "GreenGo",
    "La-Compagnie-aux", "Le-Wow-Safari", "MSC-Croisieres", "Oceania-Cruises",
    "Passager-bloque", "Prodesti-decouvrez", "Reims-les-ambitions",
    "Rivages-du-Monde", "SNCF-La-FNAUT", "Transavia-renforce",
    "Travel-Agents-Cup", "Travelib-la-plateforme", "VVF-prend",
    "Webinaire-Iceland", "Webinaire-Tourisme", "Assur-Travel",
    "A-Puylaurens", "A-Venise", "Accor-des-resultats", "Air-Canada",
    "Air-France", "Audit-de-l-Etat", "Belambra-Exotismes",
    "Charleville-Mezieres", "Club-Med-Todd", "Le-Centre-spatial",
    "Le-Club-des-Hoteliers", "Office-de-Tourisme", "Comite-du-Tourisme",
]

# Coordonnées GPS (clés en MINUSCULES)
COUNTRY_COORDS = {
    "açores": (38.7222, -27.2206),
    "afrique": (8.7832, 34.5085),
    "afrique du sud": (-30.5595, 22.9375),
    "alaska": (64.2008, -152.4937),
    "albanie": (41.1533, 20.1683),
    "algérie": (28.0339, 1.6596),
    "algerie": (28.0339, 1.6596),
    "allemagne": (51.1657, 10.4515),
    "arabie saoudite": (23.8859, 45.0792),
    "argentine": (-38.4161, -63.6167),
    "asie": (34.0479, 100.6197),
    "asie du sud-est": (10.0, 106.0),
    "autriche": (47.5162, 14.5501),
    "belize": (17.1899, -88.4976),
    "birmanie": (21.9162, 95.956),
    "bolivie": (-16.2902, -63.5887),
    "bosnie": (43.9159, 17.6791),
    "bosnie-herzégovine": (43.9159, 17.6791),
    "brésil": (-14.235, -51.9253),
    "bresil": (-14.235, -51.9253),
    "bulgarie": (42.7339, 25.4858),
    "cambodge": (12.5657, 104.991),
    "canada": (56.1304, -106.3468),
    "cap-vert": (16.5388, -23.0418),
    "cap vert": (16.5388, -23.0418),
    "chili": (-35.6751, -71.543),
    "chine": (35.8617, 104.1954),
    "chypre": (35.1264, 33.4299),
    "colombie": (4.5709, -74.2973),
    "corée du nord": (40.3399, 127.5101),
    "coree du nord": (40.3399, 127.5101),
    "corée du sud": (35.9078, 127.7669),
    "coree du sud": (35.9078, 127.7669),
    "costa rica": (9.7489, -83.7534),
    "croatie": (45.1, 15.2),
    "cuba": (21.5218, -77.7812),
    "danemark": (56.2639, 9.5018),
    "ecosse": (56.4907, -4.2026),
    "écosse": (56.4907, -4.2026),
    "egypte": (26.8206, 30.8025),
    "égypte": (26.8206, 30.8025),
    "émirats arabes unis": (23.4241, 53.8478),
    "emirats arabes unis": (23.4241, 53.8478),
    "equateur": (-1.8312, -78.1834),
    "équateur": (-1.8312, -78.1834),
    "espagne": (40.4637, -3.7492),
    "etats-unis": (37.0902, -95.7129),
    "états-unis": (37.0902, -95.7129),
    "usa": (37.0902, -95.7129),
    "finlande": (61.9241, 25.7482),
    "georgie": (42.3154, 43.3569),
    "géorgie": (42.3154, 43.3569),
    "grèce": (39.0742, 21.8243),
    "grece": (39.0742, 21.8243),
    "guatemala": (15.7835, -90.2308),
    "guyane": (3.9339, -53.1258),
    "ile de la réunion": (-21.1151, 55.5364),
    "ile de la reunion": (-21.1151, 55.5364),
    "la réunion": (-21.1151, 55.5364),
    "réunion": (-21.1151, 55.5364),
    "reunion": (-21.1151, 55.5364),
    "inde": (20.5937, 78.9629),
    "indochine": (16.0, 107.0),
    "indonésie": (-0.7893, 113.9213),
    "indonesie": (-0.7893, 113.9213),
    "irlande": (53.1424, -7.6921),
    "irlande du nord": (54.7877, -6.4923),
    "islande": (64.9631, -19.0208),
    "italie": (41.8719, 12.5674),
    "japon": (36.2048, 138.2529),
    "jordanie": (30.5852, 36.2384),
    "kenya": (-0.0236, 37.9062),
    "kosovo": (42.6026, 20.903),
    "laos": (19.8563, 102.4955),
    "macédoine": (41.5122, 21.7453),
    "macédoine du nord": (41.5122, 21.7453),
    "macedoine du nord": (41.5122, 21.7453),
    "madagascar": (-18.7669, 46.8691),
    "madère": (32.7607, -16.9595),
    "madere": (32.7607, -16.9595),
    "malaisie": (4.2105, 101.9758),
    "malte": (35.9375, 14.3754),
    "maroc": (31.7917, -7.0926),
    "mexique": (23.6345, -102.5528),
    "mongolie": (46.8625, 104.1917),
    "monténégro": (42.7087, 19.3744),
    "montenegro": (42.7087, 19.3744),
    "myanmar": (21.9162, 95.956),
    "nicaragua": (12.8654, -85.2072),
    "norvège": (60.472, 8.4689),
    "norvege": (60.472, 8.4689),
    "océan indien": (-12.0, 55.0),
    "océanie": (-22.7359, 140.0188),
    "oman": (21.4735, 55.9754),
    "ouzbékistan": (41.3775, 64.5853),
    "ouzbekistan": (41.3775, 64.5853),
    "panama": (8.538, -80.7821),
    "pérou": (-9.19, -75.0152),
    "perou": (-9.19, -75.0152),
    "philippines": (12.8797, 121.774),
    "polynésie française": (-17.6797, -149.4068),
    "polynesie francaise": (-17.6797, -149.4068),
    "polynésie": (-17.6797, -149.4068),
    "portugal": (39.3999, -8.2245),
    "pouilles": (41.0, 16.5),
    "qatar": (25.3548, 51.1839),
    "roumanie": (45.9432, 24.9668),
    "royaume-uni": (55.3781, -3.436),
    "sardaigne": (40.1209, 9.0129),
    "serbie": (44.0165, 21.0059),
    "sicile": (37.6, 14.0154),
    "slovénie": (46.1512, 14.9955),
    "slovenie": (46.1512, 14.9955),
    "sous-continent indien": (20.0, 78.0),
    "sri lanka": (7.8731, 80.7718),
    "suisse": (46.8182, 8.2275),
    "tahiti": (-17.6509, -149.426),
    "tanzanie": (-6.369, 34.8888),
    "thaïlande": (15.87, 100.9925),
    "thailande": (15.87, 100.9925),
    "tunisie": (33.8869, 9.5375),
    "turquie": (38.9637, 35.2433),
    "vietnam": (14.0583, 108.2772),
    "zanzibar": (-6.1659, 39.1989),
}

PICTO_CATEGORIES = {
    # === CLIENTÈLE (onglet "Pour qui") ===
    "amoureux": {"label": "En couple", "category": "clientele"},
    "association": {"label": "Association", "category": "clientele"},
    "ce_cse": {"label": "CE / CSE", "category": "clientele"},
    "club": {"label": "Club", "category": "clientele"},
    "famille": {"label": "En famille", "category": "clientele"},
    "gay-friendly": {"label": "Gay friendly", "category": "clientele"},
    "groupe-scolaire": {"label": "Groupes scolaires", "category": "clientele"},
    "groupes": {"label": "Groupes", "category": "clientele"},
    "handicap": {"label": "PMR", "category": "clientele"},
    "incentive": {"label": "Incentive", "category": "clientele"},
    "individuel": {"label": "En solo", "category": "clientele"},
    "seniors": {"label": "Seniors", "category": "clientele"},
    # === PRESTATIONS (onglet "Généralités") ===
    "acceuil": {"label": "Accueil", "category": "prestations"},
    "adaptabilite": {"label": "Adaptabilité", "category": "prestations"},
    "assistance": {"label": "Assistance", "category": "prestations"},
    "autotour": {"label": "Autotour", "category": "prestations"},
    "circuit": {"label": "Circuit", "category": "prestations"},
    "city": {"label": "City-break", "category": "prestations"},
    "concierge": {"label": "Conciergerie", "category": "prestations"},
    "conference": {"label": "Conférences", "category": "prestations"},
    "congres": {"label": "Congrès", "category": "prestations"},
    "creativite": {"label": "Créativité", "category": "prestations"},
    "eco_responsable": {"label": "Éco-responsable", "category": "prestations"},
    "efficacite": {"label": "Efficacité", "category": "prestations"},
    "hotel": {"label": "Hébergement", "category": "prestations"},
    "luxe": {"label": "Luxe", "category": "prestations"},
    "medical": {"label": "Médical", "category": "prestations"},
    "meeting": {"label": "Meeting", "category": "prestations"},
    "mice": {"label": "MICE", "category": "prestations"},
    "nature": {"label": "Nature", "category": "prestations"},
    "passion": {"label": "Passion", "category": "prestations"},
    "sur_mesure": {"label": "Sur-mesure", "category": "prestations"},
    "team_building": {"label": "Team-building", "category": "prestations"},
    "transferts": {"label": "Transferts", "category": "prestations"},
    "visite_guidee": {"label": "Visite guidée", "category": "prestations"},
    "vtc": {"label": "VTC", "category": "prestations"},
    # === ACTIVITÉS / THÉMATIQUES (onglet "Thématiques") ===
    "accrobranche": {"label": "Accrobranche", "category": "activites"},
    "ane": {"label": "Avec des ânes", "category": "activites"},
    "archeologie": {"label": "Archéologie", "category": "activites"},
    "architecture": {"label": "Architecture", "category": "activites"},
    "astronomie": {"label": "Astronomie", "category": "activites"},
    "attractions": {"label": "Attractions", "category": "activites"},
    "aventure": {"label": "Aventure", "category": "activites"},
    "avion-2": {"label": "Vol en avion privatisé", "category": "activites"},
    "bateau": {"label": "Navigation", "category": "activites"},
    "biathlon": {"label": "Biathlon", "category": "activites"},
    "cadeau": {"label": "Coffrets cadeaux", "category": "activites"},
    "canoe": {"label": "Canoë", "category": "activites"},
    "canyoning": {"label": "Canyoning", "category": "activites"},
    "carnaval": {"label": "Carnaval", "category": "activites"},
    "casino": {"label": "Casino", "category": "activites"},
    "char-a-voile": {"label": "Char à voile", "category": "activites"},
    "concert": {"label": "Concerts", "category": "activites"},
    "culture": {"label": "Culture", "category": "activites"},
    "culture_patrimoine": {"label": "Culture et patrimoine", "category": "activites"},
    "decouverte": {"label": "Découverte", "category": "activites"},
    "druide": {"label": "Rencontre druidique", "category": "activites"},
    "equitation": {"label": "Équitation", "category": "activites"},
    "escalade": {"label": "Escalade", "category": "activites"},
    "escalade-rando": {"label": "Escalade - Rando", "category": "activites"},
    "excursions": {"label": "Excursions", "category": "activites"},
    "feu_d_artifice": {"label": "Feu d'artifice", "category": "activites"},
    "genealogie": {"label": "Généalogie", "category": "activites"},
    "golf": {"label": "Golf", "category": "activites"},
    "histoire": {"label": "Histoire / Châteaux", "category": "activites"},
    "illuminations": {"label": "Illuminations", "category": "activites"},
    "insecte": {"label": "Entomologie", "category": "activites"},
    "jardin": {"label": "Jardin", "category": "activites"},
    "kite": {"label": "Kitesurf", "category": "activites"},
    "liguistique": {"label": "Linguistique", "category": "activites"},
    "livre": {"label": "Histoire", "category": "activites"},
    "manuel": {"label": "Artisanat", "category": "activites"},
    "massage": {"label": "Bien-être / Thalasso", "category": "activites"},
    "mode": {"label": "Mode", "category": "activites"},
    "montagne": {"label": "Lacs et montagne", "category": "activites"},
    "montgolfiere": {"label": "Montgolfière", "category": "activites"},
    "noel": {"label": "Marchés de Noël", "category": "activites"},
    "nouvel_an": {"label": "Nouvel an", "category": "activites"},
    "observation_animale": {"label": "Observation animale", "category": "activites"},
    "oenologie": {"label": "Œnologie", "category": "activites"},
    "peche": {"label": "Pêche en mer", "category": "activites"},
    "peinture": {"label": "Aquarelle", "category": "activites"},
    "permaculture": {"label": "Permaculture", "category": "activites"},
    "photo": {"label": "Photographie", "category": "activites"},
    "plongee": {"label": "Plongée", "category": "activites"},
    "rafting": {"label": "Rafting", "category": "activites"},
    "rando": {"label": "Randonnée", "category": "activites"},
    "rando-contee": {"label": "Balade contée", "category": "activites"},
    "raquettes": {"label": "Raquettes", "category": "activites"},
    "resto": {"label": "Gastronomie", "category": "activites"},
    "safari2": {"label": "Safari", "category": "activites"},
    "shooting_photo": {"label": "Photographie", "category": "activites"},
    "ski": {"label": "Ski", "category": "activites"},
    "ski-de-fond": {"label": "Ski de randonnée", "category": "activites"},
    "slow_tourisme": {"label": "Slow tourisme", "category": "activites"},
    "snorkeling": {"label": "Snorkeling", "category": "activites"},
    "soiree_gala": {"label": "Soirées de gala", "category": "activites"},
    "source-eau-chaude": {"label": "Bains d'eaux chaudes", "category": "activites"},
    "spirituel": {"label": "Spirituel", "category": "activites"},
    "sport": {"label": "Sport", "category": "activites"},
    "surf": {"label": "Surf", "category": "activites"},
    "surpris": {"label": "Insolite", "category": "activites"},
    "thematique": {"label": "Multi activités", "category": "activites"},
    "tourisme_responsable2": {"label": "Tourisme responsable", "category": "activites"},
    "trail": {"label": "Trail", "category": "activites"},
    "train": {"label": "Petit train", "category": "activites"},
    "traineau": {"label": "Mushing", "category": "activites"},
    "trekking": {"label": "Trekking", "category": "activites"},
    "velo": {"label": "Vélo", "category": "activites"},
    "vendanges": {"label": "Vendanges", "category": "activites"},
    "viaferrata": {"label": "Via Ferrata", "category": "activites"},
    "video": {"label": "Vidéo", "category": "activites"},
    "village": {"label": "Village", "category": "activites"},
    "voyage": {"label": "Voyage", "category": "activites"},
    "voyage_de_noce": {"label": "Voyage de noces", "category": "activites"},
    "yoga": {"label": "Yoga", "category": "activites"},
}

CONTINENT_MAP = {
    "açores": "Europe", "afrique du sud": "Afrique", "alaska": "Amériques",
    "albanie": "Europe", "algérie": "Afrique", "algerie": "Afrique",
    "allemagne": "Europe", "arabie saoudite": "Moyen-Orient",
    "argentine": "Amériques", "autriche": "Europe", "belize": "Amériques",
    "bolivie": "Amériques", "bosnie": "Europe", "bosnie-herzégovine": "Europe",
    "brésil": "Amériques", "bresil": "Amériques", "bulgarie": "Europe",
    "cambodge": "Asie", "canada": "Amériques", "cap-vert": "Afrique",
    "cap vert": "Afrique", "chili": "Amériques", "chine": "Asie",
    "chypre": "Europe", "colombie": "Amériques", "corée du nord": "Asie",
    "coree du nord": "Asie", "corée du sud": "Asie", "coree du sud": "Asie",
    "costa rica": "Amériques", "croatie": "Europe", "cuba": "Amériques",
    "danemark": "Europe", "ecosse": "Europe", "écosse": "Europe",
    "egypte": "Afrique", "égypte": "Afrique",
    "émirats arabes unis": "Moyen-Orient", "emirats arabes unis": "Moyen-Orient",
    "equateur": "Amériques", "équateur": "Amériques", "espagne": "Europe",
    "etats-unis": "Amériques", "états-unis": "Amériques", "usa": "Amériques",
    "finlande": "Europe", "georgie": "Europe", "géorgie": "Europe",
    "grèce": "Europe", "grece": "Europe", "guatemala": "Amériques",
    "guyane": "Amériques", "ile de la réunion": "Océan Indien",
    "ile de la reunion": "Océan Indien", "la réunion": "Océan Indien",
    "réunion": "Océan Indien", "reunion": "Océan Indien",
    "inde": "Asie", "indochine": "Asie",
    "indonésie": "Asie", "indonesie": "Asie", "irlande": "Europe",
    "irlande du nord": "Europe", "islande": "Europe", "italie": "Europe",
    "japon": "Asie", "jordanie": "Moyen-Orient", "kenya": "Afrique",
    "kosovo": "Europe", "laos": "Asie", "macédoine": "Europe",
    "macédoine du nord": "Europe", "macedoine du nord": "Europe",
    "madagascar": "Afrique", "madère": "Europe", "madere": "Europe",
    "malaisie": "Asie", "malte": "Europe", "maroc": "Afrique",
    "mexique": "Amériques", "mongolie": "Asie", "monténégro": "Europe",
    "montenegro": "Europe", "myanmar": "Asie", "nicaragua": "Amériques",
    "norvège": "Europe", "norvege": "Europe", "oman": "Moyen-Orient",
    "ouzbékistan": "Asie", "ouzbekistan": "Asie", "panama": "Amériques",
    "pérou": "Amériques", "perou": "Amériques", "philippines": "Asie",
    "polynésie française": "Océanie", "polynesie francaise": "Océanie",
    "polynésie": "Océanie", "portugal": "Europe", "pouilles": "Europe",
    "qatar": "Moyen-Orient", "roumanie": "Europe", "royaume-uni": "Europe",
    "sardaigne": "Europe", "serbie": "Europe", "sicile": "Europe",
    "slovénie": "Europe", "slovenie": "Europe", "sri lanka": "Asie",
    "suisse": "Europe", "tahiti": "Océanie", "tanzanie": "Afrique",
    "thaïlande": "Asie", "thailande": "Asie", "tunisie": "Afrique",
    "turquie": "Europe", "vietnam": "Asie", "zanzibar": "Afrique",
}


# =============================================================================
# FONCTIONS
# =============================================================================

def fetch_page(url, retries=3):
    """Télécharge une page HTML avec gestion des erreurs et retries."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            print(f"  [WARN] Tentative {attempt + 1}/{retries} échouée pour {url}: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    print(f"  [ERROR] Impossible de charger {url}")
    return None


def is_news_article(url):
    """Vérifie si une URL correspond à un article d'actualité."""
    slug = url.split("/")[-1]
    for pattern in NEWS_PATTERNS:
        if pattern in slug:
            return True
    return False


def extract_dmc_links(html):
    """Extrait les liens vers les fiches DMC depuis la page annuaire."""
    blocks = re.finditer(
        r'<div\s+class="art-(\d+)\s+cel1[^"]*"[^>]*>(.*?)(?=<div\s+class="art-\d+\s+cel1|$)',
        html, re.DOTALL,
    )
    dmc_links = []
    seen_urls = set()
    for match in blocks:
        content = match.group(2)
        link_match = re.search(r'href="(/[^"]*_a(\d+)\.html)"', content)
        if not link_match:
            continue
        url_path = link_match.group(1)
        if url_path in seen_urls:
            continue
        seen_urls.add(url_path)
        full_url = BASE_URL + url_path
        if is_news_article(full_url):
            continue
        dmc_links.append(full_url)
    return dmc_links


def get_coords(destination):
    """Trouve les coordonnées GPS (insensible à la casse + correspondance partielle)."""
    dest_lower = destination.lower().strip()
    if dest_lower in COUNTRY_COORDS:
        return COUNTRY_COORDS[dest_lower]
    for key, coords in COUNTRY_COORDS.items():
        if key in dest_lower or dest_lower in key:
            return coords
    return None, None


def get_continent(destination):
    """Trouve le continent (insensible à la casse + correspondance partielle)."""
    dest_lower = destination.lower().strip()
    if dest_lower in CONTINENT_MAP:
        return CONTINENT_MAP[dest_lower]
    for key, continent in CONTINENT_MAP.items():
        if key in dest_lower or dest_lower in key:
            return continent
    return None


def clean_destinations(destinations):
    """
    Nettoie la liste de destinations :
    - Corrige les entrées mal parsées vers le bon pays
    - Supprime les entrées qui ne sont pas des pays/destinations valides
    """
    # Mapping de corrections : entrées mal parsées → bon pays
    CORRECTIONS = {
        "equateur - amazonie - galapagos": "Equateur",
        "equateur-amazonie-galapagos": "Equateur",
        "toute l'islande": "Islande",
        "toute l islande": "Islande",
        "circuits combines turquie": "Grèce",
        "circuits combinés turquie": "Grèce",
        "turquiegrèce": "Grèce",
        "turquiegrece": "Grèce",
        "turquie grèce": "Grèce",
        "turquie grece": "Grèce",
        "cambodge dat": "Cambodge",
        "vietna": "Vietnam",
        "costa": "Costa Rica",
        "usa": "Etats-Unis",
    }

    # Entrées à supprimer (régions, villes, bouts de phrase — pas des pays)
    BLACKLIST = {
        "baja california", "chiapas", "oaxaca", "yucatan", "quintana roo",
        "quitana roo", "mexico city", "londres", "london",
        "sud-est", "sud-ouest", "angleterre",
        "pouilles", "polynésie", "polynesie",
        "europe de l'est", "europe de l est",
        "fjords", "glaciers",
    }

    cleaned = []
    for d in destinations:
        # Normaliser les apostrophes typographiques → droites
        d_norm = d.replace("\u2019", "'").replace("\u2018", "'").replace("\u00b4", "'")
        d_lower = d_norm.lower().strip()

        # Vérifier les corrections (match partiel)
        corrected = None
        for pattern, replacement in CORRECTIONS.items():
            if pattern in d_lower:
                corrected = replacement
                break

        if corrected:
            if corrected not in cleaned:
                cleaned.append(corrected)
            continue

        # Vérifier la blacklist (match partiel)
        is_blacklisted = False
        for bl in BLACKLIST:
            if bl in d_lower:
                is_blacklisted = True
                break
        # Blacklister les entrées trop longues (probablement des phrases)
        if len(d) > 40:
            is_blacklisted = True
        # Blacklister les entrées avec des parenthèses (descriptions)
        if "(" in d and ")" in d:
            before_paren = d.split("(")[0].strip().rstrip(" ,;:")
            if before_paren and len(before_paren) > 2 and before_paren not in cleaned:
                cleaned.append(before_paren)
            is_blacklisted = True

        if not is_blacklisted:
            cleaned.append(d)

    return cleaned


def extract_destinations(html):
    """Extrait les destinations depuis une fiche DMC (plusieurs méthodes de fallback)."""
    destinations = []

    # Méthode 1 : bloc "DESTINATIONS :" standard
    dest_match = re.search(
        r"DESTINATIONS\s*:\s*(.*?)(?:Date de cr|<div class=\"clear\"|</div>)",
        html, re.DOTALL | re.IGNORECASE,
    )
    if dest_match:
        dest_text = re.sub(r"<[^>]+>", " ", dest_match.group(1))
        dest_text = dest_text.replace("&gt;", ">").replace("&amp;", "&").replace("&nbsp;", " ")
        dests_raw = re.findall(r">\s*([A-ZÀ-Üa-zà-ü][^>]*?)(?=\s*>|\s*$)", dest_text)
        for d in dests_raw:
            # Nettoyage agressif des résidus
            d = d.strip()
            d = re.sub(r'["\u201c\u201d\u00ab\u00bb]', '', d)  # Guillemets
            d = re.sub(r'\.{2,}', '', d)  # Points de suspension
            d = d.rstrip(".,;:!? ")
            d = d.lstrip(".,;:!? /\\>")
            # Supprimer les résidus de "Date D..." ou "Date de..."
            if re.match(r'^Date\b', d, re.IGNORECASE):
                continue
            # Ignorer les entrées trop courtes ou qui ressemblent à du bruit
            if not d or len(d) < 2 or d.lower() in ('d', 'de', 'du', 'et', 'en', 'la', 'le', 'les'):
                continue
            destinations.append(d)

    # Méthode 2 : chercher dans og:title ("DMC Pays Nom" ou similaire)
    if not destinations:
        title_match = re.search(r'og:title"\s*content="([^"]*)"', html)
        if title_match:
            title = title_match.group(1).lower()
            for key in sorted(COUNTRY_COORDS.keys(), key=len, reverse=True):
                if key in title:
                    destinations.append(key.title())

    # Méthode 3 : chercher les pays connus dans l'URL
    if not destinations:
        url_match = re.search(r'canonical"\s*href="([^"]*)"', html)
        url_str = url_match.group(1).lower() if url_match else ""
        for key in sorted(COUNTRY_COORDS.keys(), key=len, reverse=True):
            key_slug = key.replace(" ", "-").replace("'", "-")
            if key_slug in url_str:
                destinations.append(key.title())
                break

    return destinations


def normalize_destination(d):
    """Normalise un nom de destination en Title Case avec gestion des petits mots."""
    # Nettoyage supplémentaire
    d = re.sub(r'["\u201c\u201d\u00ab\u00bb]', '', d)
    d = re.sub(r'\.{2,}', '', d)
    d = d.strip().rstrip(".,;:!? ").lstrip(".,;:!? /\\>")
    if not d:
        return ""
    
    # Mapping canonique pour unifier les variantes avec/sans accents
    CANONICAL = {
        "bresil": "Brésil",
        "egypte": "Égypte",
        "ecosse": "Écosse",
        "equateur": "Équateur",
        "etats-unis": "États-Unis",
        "états-unis": "États-Unis",
        "emirats arabes unis": "Émirats Arabes Unis",
        "émirats arabes unis": "Émirats Arabes Unis",
        "georgie": "Géorgie",
        "géorgie": "Géorgie",
        "grece": "Grèce",
        "grèce": "Grèce",
        "madere": "Madère",
        "madère": "Madère",
        "coree du nord": "Corée du Nord",
        "corée du nord": "Corée du Nord",
        "coree du sud": "Corée du Sud",
        "corée du sud": "Corée du Sud",
        "macedoine du nord": "Macédoine du Nord",
        "macédoine du nord": "Macédoine du Nord",
        "montenego": "Monténégro",
        "montenegro": "Monténégro",
        "monténégro": "Monténégro",
        "norvege": "Norvège",
        "norvège": "Norvège",
        "ouzbekistan": "Ouzbékistan",
        "ouzbékistan": "Ouzbékistan",
        "perou": "Pérou",
        "pérou": "Pérou",
        "polynesie francaise": "Polynésie Française",
        "polynésie française": "Polynésie Française",
        "reunion": "Réunion",
        "réunion": "Réunion",
        "ile de la reunion": "Île de la Réunion",
        "ile de la réunion": "Île de la Réunion",
        "slovenie": "Slovénie",
        "slovénie": "Slovénie",
        "thailande": "Thaïlande",
        "thaïlande": "Thaïlande",
        "indonesie": "Indonésie",
        "indonésie": "Indonésie",
        "algerie": "Algérie",
        "algérie": "Algérie",
    }
    
    # Vérifier le mapping canonique d'abord
    d_lower = d.lower().strip()
    if d_lower in CANONICAL:
        return CANONICAL[d_lower]
    
    # Supprimer les résidus de "Date" qui auraient pu passer
    d = re.sub(r'\s*Date\b.*$', '', d, flags=re.IGNORECASE).strip()
    if not d or len(d) < 2:
        return ""
    
    words = d.split()
    result = []
    for i, w in enumerate(words):
        wl = w.lower()
        if i == 0:
            result.append(w.capitalize())
        elif wl in ('du', 'de', 'la', 'le', 'les', 'et', 'des', 'en', "l'", "d'"):
            result.append(wl)
        else:
            result.append(w.capitalize())
    return ' '.join(result)


def extract_primary_destinations(title):
    """
    Extrait la/les destination(s) principale(s) depuis le og:title.
    Le titre est souvent au format : "DMC [Pays] [Nom du DMC]"
    Renvoie la liste des pays reconnus dans le titre.
    """
    if not title:
        return []

    title_lower = title.lower()
    found = []
    # Trier par longueur décroissante pour matcher les noms composés d'abord
    sorted_keys = sorted(COUNTRY_COORDS.keys(), key=len, reverse=True)
    remaining = title_lower
    for key in sorted_keys:
        if key in remaining:
            found.append(key)
            # Retirer le match pour éviter les doublons partiels
            remaining = remaining.replace(key, ' ', 1)
    return found


def normalize_title(title):
    """
    Normalise un titre de DMC en Title Case cohérent.
    Ex: 'DMC BRESIL BRAZIL SENSATIONS' → 'DMC Brésil Brazil Sensations'
        'Phoenix Voyages Réceptif Vietnam' → 'Phoenix Voyages Réceptif Vietnam'
    """
    if not title:
        return ""
    # Mots qui restent en minuscule
    small_words = {'du', 'de', 'la', 'le', 'les', 'et', 'des', 'en', 'au', 'aux', 'un', 'une', 'à', 'a'}
    # Mots/acronymes qui restent en majuscule
    upper_words = {'dmc', 'usa', 'vtc', 'mice'}
    
    words = title.split()
    result = []
    for i, w in enumerate(words):
        wl = w.lower()
        if wl in upper_words:
            result.append(w.upper())
        elif i > 0 and wl in small_words:
            result.append(wl)
        else:
            result.append(w.capitalize())
    return ' '.join(result)


def extract_dmc_data(html, url):
    """Extrait les données structurées d'une fiche DMC."""
    data = {"url": url}


    # Titre
    title_match = re.search(r'og:title"\s*content="([^"]*)"', html)
    raw_title = title_match.group(1).strip() if title_match else ""
    data["title"] = normalize_title(raw_title)

    # Description
    desc_match = re.search(r'og:description"\s*content="([^"]*)"', html)
    desc = desc_match.group(1).strip() if desc_match else ""
    # Nettoyer : supprimer tout à partir de "DESTINATIONS :" si présent dans la description
    desc = re.split(r'\s*DESTINATIONS\s*:', desc, flags=re.IGNORECASE)[0].strip()
    # Supprimer aussi "Date de création" si ça traîne
    desc = re.split(r'\s*Date de cr[ée]ation\s*:', desc, flags=re.IGNORECASE)[0].strip()
    data["description"] = desc

    # Image
    img_match = re.search(r'og:image"\s*content="([^"]*)"', html)
    data["image"] = img_match.group(1).strip() if img_match else ""

    # ---- DESTINATIONS ----
    # 1. Toutes les destinations listées dans la fiche (pour filtrage/affichage)
    all_destinations_raw = extract_destinations(html)
    all_destinations = clean_destinations(all_destinations_raw)
    normalized_all = []
    seen_dests = set()
    for d in all_destinations:
        nd = normalize_destination(d)
        if nd and len(nd) > 1 and nd.lower() not in seen_dests:
            seen_dests.add(nd.lower())
            normalized_all.append(nd)
    data["destinations"] = normalized_all

    # 2. Destination(s) principale(s) = celle(s) de CETTE fiche spécifique
    #    Extraites du og:title qui contient le nom du pays de la fiche
    primary_raw = extract_primary_destinations(data["title"])

    # Fallback: si rien trouvé dans le titre, essayer dans l'URL
    if not primary_raw:
        url_slug = url.split("/")[-1].lower()
        sorted_keys = sorted(COUNTRY_COORDS.keys(), key=len, reverse=True)
        for key in sorted_keys:
            key_slug = key.replace(" ", "-").replace("'", "-")
            if key_slug in url_slug:
                primary_raw.append(key)
                break

    # Fallback final: si toujours rien, utiliser toutes les destinations
    if not primary_raw:
        primary_raw = [d.lower().strip() for d in all_destinations]

    primary_normalized = []
    seen_primary = set()
    for d in primary_raw:
        nd = normalize_destination(d)
        if nd and len(nd) > 1 and nd.lower() not in seen_primary:
            seen_primary.add(nd.lower())
            primary_normalized.append(nd)
    data["primary_destinations"] = primary_normalized

    # Coordonnées GPS = UNIQUEMENT les destinations principales (pour les marqueurs)
    coords_list = []
    for dest in primary_raw:
        lat, lng = get_coords(dest)
        norm_dest = normalize_destination(dest)
        coords_list.append({"destination": norm_dest, "lat": lat, "lng": lng})
        if lat is None:
            print(f"  [WARN] Pas de coordonnées pour: '{dest}'")
    data["coordinates"] = coords_list

    # Continents (basés sur TOUTES les destinations pour le filtrage)
    continents = set()
    for dest in all_destinations:
        continent = get_continent(dest)
        if continent:
            continents.add(continent)
    data["continents"] = sorted(list(continents))

    # Date de création
    date_match = re.search(
        r"Date de cr[ée]ation\s*:?\s*</b>\s*(?:<br\s*/?>)?\s*\n?\s*(.+?)(?:\n|\s*<br)",
        html,
    )
    if date_match:
        date_text = re.sub(r"<[^>]+>", "", date_match.group(1)).strip()
        data["date_creation"] = date_text
    else:
        data["date_creation"] = ""

    # Pictogrammes / Tags
    pictos_raw = re.findall(r"docs/FicheDMC/picto_([^\"\.]+)", html)
    seen = set()
    pictos = []
    for p in pictos_raw:
        if p not in seen:
            seen.add(p)
            pictos.append(p)

    tags = {"clientele": [], "prestations": [], "activites": []}
    for picto_id in pictos:
        if picto_id in PICTO_CATEGORIES:
            info = PICTO_CATEGORIES[picto_id]
            tags[info["category"]].append({"id": picto_id, "label": info["label"]})
        else:
            tags["activites"].append({
                "id": picto_id,
                "label": picto_id.replace("_", " ").replace("-", " ").title(),
            })
    data["tags"] = tags

    return data


def is_dmc_fiche(html):
    """
    Vérifie qu'une page est bien une fiche DMC.
    Accepte si la page a DESTINATIONS, des pictos, ou des mots-clés DMC.
    """
    has_destinations = bool(re.search(r"DESTINATIONS\s*:", html, re.IGNORECASE))
    has_pictos = bool(re.search(r"docs/FicheDMC/picto_", html))
    has_dmc_keywords = bool(re.search(
        r"(agence\s+r[ée]ceptive|DMC|r[ée]ceptif|voyage[s]?\s+sur[- ]mesure)",
        html, re.IGNORECASE,
    ))
    return has_destinations or has_pictos or has_dmc_keywords


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("SCRAPING DMC - DestiMaG / TourMaG")
    print(f"Démarré le {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    print("\n[1/3] Chargement de la page annuaire...")
    annuaire_html = fetch_page(ANNUAIRE_URL)
    if not annuaire_html:
        print("ERREUR: Impossible de charger la page annuaire. Abandon.")
        sys.exit(1)

    print("[2/3] Extraction des liens vers les fiches DMC...")
    all_links = extract_dmc_links(annuaire_html)
    print(f"  → {len(all_links)} liens trouvés (après exclusion des articles d'actu)")

    print(f"[3/3] Scraping de chaque fiche DMC (délai de {REQUEST_DELAY}s entre chaque)...")
    dmc_list = []
    skipped = 0
    skipped_urls = []

    for i, link in enumerate(all_links, 1):
        print(f"  [{i}/{len(all_links)}] {link}")
        time.sleep(REQUEST_DELAY)

        html = fetch_page(link)
        if not html:
            skipped += 1
            skipped_urls.append({"url": link, "reason": "Erreur de chargement"})
            continue

        if not is_dmc_fiche(html):
            print(f"    → Pas une fiche DMC, ignoré.")
            skipped += 1
            skipped_urls.append({"url": link, "reason": "Pas identifié comme fiche DMC"})
            continue

        dmc_data = extract_dmc_data(html, link)
        dmc_list.append(dmc_data)
        dest_str = ", ".join(dmc_data["destinations"]) if dmc_data["destinations"] else "(aucune destination)"
        print(f"    → OK: {dmc_data['title']} ({dest_str})")

    # Générer le JSON
    output = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": ANNUAIRE_URL,
            "total_dmc": len(dmc_list),
            "total_links_found": len(all_links),
            "skipped": skipped,
            "skipped_urls": skipped_urls,
        },
        "dmc": dmc_list,
    }

    import os
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print(f"TERMINÉ !")
    print(f"  → {len(dmc_list)} fiches DMC extraites")
    print(f"  → {skipped} liens ignorés")
    if skipped_urls:
        print(f"  → URLs ignorées :")
        for s in skipped_urls:
            print(f"      {s['url']} ({s['reason']})")
    print(f"  → Fichier généré : {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
