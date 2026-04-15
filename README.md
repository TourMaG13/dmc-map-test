# DMC Map - Carte interactive des DMC DestiMaG

Carte interactive des agences réceptives (DMC) référencées dans l'annuaire [DestiMaG / TourMaG](https://www.tourmag.com/Annuaire-des-agences-touristiques-locales_r404.html).

## Fonctionnement

### Scraping automatique
Le script `scrape_dmc.py` parcourt automatiquement l'annuaire DestiMaG, scrape chaque fiche DMC et génère le fichier `data/dmc_data.json`.

Un workflow GitHub Actions exécute ce script **4 fois par jour** (08h, 11h, 14h, 17h heure de Paris). Si des changements sont détectés (ajout, modification ou suppression de fiches), le JSON est automatiquement mis à jour dans le repo.

### Données extraites pour chaque DMC
- Nom et description
- Image principale
- Destinations couvertes (avec coordonnées GPS)
- Date de création
- Tags organisés en 3 catégories : clientèle, prestations, activités
- Lien vers la fiche complète sur TourMaG

### Carte interactive (widget)
La carte est hébergée sur GitHub Pages et peut être intégrée sur le site TourMaG via une iframe.

## Structure du repo

```
dmc-map/
├── .github/workflows/
│   └── scrape.yml          # Workflow GitHub Actions (scraping auto)
├── data/
│   └── dmc_data.json       # Données DMC (auto-généré)
├── scrape_dmc.py            # Script de scraping Python
└── README.md
```

## Lancer le scraping manuellement

```bash
python scrape_dmc.py
```

Ou depuis l'interface GitHub : Actions → Scraping DMC DestiMaG → Run workflow.
