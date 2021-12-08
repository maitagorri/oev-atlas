oev_atlas
==============================

Processing data & setting up website for Agora Verkehrswende OV-Atlas



Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── interim        <- Intermediate data that has been transformed.
    |   |    ├── geo       <- geo-data
    │   |    └── gtfs      <- GTFS databases etc.
    │   ├── processed      <- The final data sets to be mapped/reported.
    │   └── raw            <- Data as downloaded/obtained from external sources.
    │        ├── bkg       <- Bundesamt für Kartographie & Geodäsie 
    │        ├── delfi     <- DELFI
    │        ├── gtfs.de   <- gtfs.de (Brosi--Gesamtdatensatz nicht öffentlich)
    │        └── inkar     <- Indikatoren und Karten zur Raum- und Stadtentwicklung
    │
    ├── environment.yml    <- The requirements file for reproducing the analysis environment, set 
    │                         up with `conda env create -f environment.yml`
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │
    ├── scrap              <- Notebooks & exploratory code. These are not guaranteed to work with
    │                         the files as they are organized now, but kept as a repository for 
    │                         useful auxiliary analyses.
    │
    └── src                <- Source code for use in this project. Needs manual interference!
        │
        ├── processing     <- Scripts to extract and turn data into useable (geo-)formats
        │   ├── 1_processstops-db.ipynb
        │   ├── 2_pointcount_full-dataset.py
        │   └── verkehrsanteile_cleaning.ipynb
        │
        ├── report         <- Scripts to produce preliminary reports from GTFS
        │   └── automatisierte-grafiken.Rmd
        │
        └── webmap         <- HTML to embed Mapbox Style in website
            └── gapmap.mapboxgl.html
    

Notes on data
------------
- Detail on data sources is document under [references](https://github.com/maitagorri/oev-atlas/blob/organizing_files/references/data_sources.md)
- Neither raw nor processed data are shared here, because of their size and, for the most part, public availability
    - One exception are mode shares from SrV 2018, since I had to manually copy them out of a pdf


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
