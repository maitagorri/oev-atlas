oev_atlas
==============================

Processing data & setting up website for Agora Verkehrswende OV-Atlas

Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final data sets to be mapped/reported.
    │   └── raw            <- Data as downloaded/obtained from external sources.
    │
    ├── environment.yml    <- The requirements file for reproducing the analysis environment, set 
    │                         up with `conda env create -f environment.yml`
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │
    ├── scrap              <- Notebooks & exploratory code. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    └── src                <- Source code for use in this project.
        │
        ├── processing     <- Scripts to extract and turn data into useable (geo-)formats
        │   └── make_dataset.py
        │
        ├── report         <- Scripts to produce preliminary reports from GTFS
        │   └── build_features.py
        │
        └── webmap         <- HTML to embed Mapbox Style in website
            └── visualize.py
    

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
