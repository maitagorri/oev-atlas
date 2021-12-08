oev_atlas
==============================

Raw data and Sources
------------

### bkg       
Verwaltungsgebiete nach [Bundesamt für Kartographie & Geodäsie](https://gdz.bkg.bund.de/index.php/default/verwaltungsgebiete-1-250-000-ebenen-stand-01-01-vg250-ebenen-01-01.html)

#### Verwaltungsgebiete 1:250 000 (Ebenen), Stand 01.01. (VG250 01.01.) Georeferenzierung: UTM32s
 
    [Download link](https://daten.gdz.bkg.bund.de/produkte/vg/vg250_ebenen_0101/aktuell/vg250_01-01.utm32s.shape.ebenen.zip)

    > Der Datenbestand umfasst sämtliche Verwaltungseinheiten der hierarchischen Verwaltungsebenen vom Staat bis zu den Gemeinden mit ihren Grenzen, statistischen Schlüsselzahlen, Namen der Verwaltungseinheit sowie die spezifische Bezeichnung der Verwaltungsebene des jeweiligen Landes.

    > Das Produkt VG250-EW enthält zusätzlich Einwohnerzahlen. Die Geometrie der Grenzen ist hinsichtlich Genauigkeit und Auflösung auf das ATKIS®-DLM250 ausgerichtet.

    - Quelle für Geographie, Katasterfläche (km²), und Einwohnerzahl
    - *Einwohnerzahl* `EWZ` -- Es handelt sich um die Einwohnerzahlen des Statistischen Bundesamtes
    (www.destatis.de) mit dem Stand des 31.12. des jeweiligen Jahres.
    - *Katasterfläche* `KFL` -- Angabe der Katasterflächen in km2 aus der Flächenstatistik des Statistischen
    Bundesamtes (www.destatis.de) mit dem Stand des 31.12. des jeweiligen
    Jahres.


### delfi 
Sollfahrplandaten nach [DELFI](https://www.opendata-oepnv.de/ht/de/willkommen)

#### Aktuelle Sollfahrplandaten
    [DELFI-Portal zum Download](https://www.opendata-oepnv.de/ht/de/organisation/delfi/startseite?tx_vrrkit_view%5Bdataset_name%5D=deutschlandweite-sollfahrplandaten-gtfs&tx_vrrkit_view%5Baction%5D=details&tx_vrrkit_view%5Bcontroller%5D=View)
    
    - Fahrplandaten im GTFS-Format
    - Jeweils für verbleibende Fahrplanperiode
    - Login zum Download notwendig

### gtfs.de
Sollfahrplandaten (kostenpflichtig) nach [gtfs.de](https://gtfs.de/de/feeds/)

#### Vollständige Sollfahrplandaten 
    - Aufbereitet im GTFS-Format
    - Gesamtdatensatz kostenpflichtig
    - Wöchentliche Feeds kostenlos herunterladbar
    - FV-Routen im Gesamtdatensatz können durch `route_names` aus dem wöchentlichen Feed identifiziert werden

### inkar     
[Indikatoren und Karten zur Raum- und Stadtentwicklung](https://www.inkar.de/Default)

#### Siedlungsfläche
    - Indikatoren müssen für jede Verwaltungsebene (Kreise, Gemeinden) manuell ausgewählt und heruntergeladen werden


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
