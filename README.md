# KV_RV_25

### Struktur
Dette repository indeholder scripts til at hente og strukturere data for kommunalvalg og regionsvalg 2025 i Danmark. Dataene hentes fra kombits offentlige SFTP forbindelse og struktureres i et format, der er nemt at analysere og bruge til videre formål. En del af filerne er også direkte datainput til Altingets valgvisualiseringer.

#### `data/`
- **`raw/`** : Indeholder de rå datafiler hentet direkte fra kombits SFTP server. Filstrukturen her spejler den, der findes på SFTP serveren, med undermapper for `kandidat-data`, `valgresultater`, `mandatfordeling`, `valgdeltagelse` og `verifikation` (til midlertidige "testfiler").
- **`struktureret/`** : Indeholder de strukturerede datafiler efter behandling af scripts. Her findes separate filer for kommunalvalg og regionsvalg, opdelt i partier og kandidater.
    - `kv/` : Strukturerede data for kommunalvalg 2025
        - `kv25_resultater_partier.csv` : Valgresultater på partiniveau
        - `kv25_resultater_kandidater.csv` : Valgresultater på kandidat
        - `valgresultater/afstemningssteder/` : indeholder en fil per kommune med valgresultater på afstemningsstedsniveau. Skal bruges til visualiseringer på afstemningsstedsniveau.
        - `valgresultater/kommuner` : indeholder en fil per kommune med valgresultater på kommuneniveau. Skal bruges til visualiseringer på kommuneniveau.
        - `kandidat-info/` : Indeholder to filer: én med kandidatdata og én med valgforbundsdata for kommunalvalget 2025.
    - `rv/` : Strukturerede data for regionsvalg 2025
        - `parti-resultater/` : Valgresultater på partiniveau
        - `kandidat-resultater/` : Valgresultater på kandidatniveau
- **`kommuner.json`** : En JSON-fil, der indeholder information om danske kommuner, herunder deres tilknytning til regioner, dagi id og id numre til de tilhørende grafikker
- **`shapes`** : Mappe med geografiske filer over hvert afstemningsområde i GeoJSON og TopoJSON format. Der er en fil per kommune. 

### Scrips
- **`01_hent_data.py`** : Forbinder til kombits offentlige SFTP forbindelse og henter de rå datafiler for kommunalvalg og regionsvalg 2025. De bliver gemt i mappen 'data/raw' efter sammen undermappestruktur som på SFTP serveren (`kandidat-data`, `valgresultater`, `mandatfordeling`, `valgdeltagelse` og mappen `verifikation` til de midlertidige "testfiler").
- **`02a_strukturer_kv25_resultater.py`** : Strukturerer de resultater, der er hentet for kommunalvalget 2025. Scriptet genererer to forskellige filer: én for partiernes resultater og én for kandidaternes resultater. Begge er på valgstedsniveau.
- **`02b_strukturer_kv25_kandidatdata.py`** : Strukturerer data på kandidater og valgforbundet. Begge filer genereres for at journalister og andre brugere nemt kan få adgang til kandidatdata for kommunalvalget 2025.
- **`03a_strukturer_rv25_resultater.py`** : Strukturering af regionsvalg 2025 resultater. Strukturerer de resultater, der er hentet for regionsrådsvalget 2025. Scriptet genererer to forskellige filer: én for partiernes resultater og én for kandidaternes resultater. Begge er på valgstedsniveau. Scriptet trækker også på filen `data/kommuner.json` for at tilføje regionsinformation baseret på kommuneinformation.
