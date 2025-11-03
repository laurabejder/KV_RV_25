# KV_RV_25


### Scrips
- **`01_hent_data.py`** : Forbinder til kombits offentlige SFTP forbindelse og henter de rå datafiler for kommunalvalg og regionsvalg 2025. De bliver gemt i mappen 'data/raw' efter sammen undermappestruktur som på SFTP serveren (`kandidat-data`, `valgresultater`, `mandatfordeling`, `valgdeltagelse` og mappen `verifikation` til de midlertidige "testfiler").
- **`02a_strukturer_kv25_resultater.py`** : Strukturerer de resultater, der er hentet for kommunalvalget 2025. Scriptet genererer to forskellige filer: én for partiernes resultater og én for kandidaternes resultater. Begge er på valgstedsniveau.

- **`03a_strukturer_rv25_resultater.py`** : Strukturering af regionsvalg 2025 resultater. Strukturerer de resultater, der er hentet for regionsrådsvalget 2025. Scriptet genererer to forskellige filer: én for partiernes resultater og én for kandidaternes resultater. Begge er på valgstedsniveau. Scriptet trækker også på filen `data/kommuner.json` for at tilføje regionsinformation baseret på kommuneinformation.