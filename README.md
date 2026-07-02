# Påmindelse om afregning af indsatser – voksne

Robotten overvåger KMD Nexus for nyligt ændrede voksne indsatser og opretter opgaver af typen "Indsatser til økonomi – voksne" til Regnskab BSF, så økonomiafdelingen notificeres, når en indsats skifter tilstand og endnu ikke er registreret i regnskabssystemet.

## Hvad gør robotten?

1. Indlæser regler fra en lokal Excel-fil (`Regelsæt.xlsx`) med relevante organisationer, indsatsstatusser og irrelevante leverandører.
2. Forespørger Nexus-databasen på indsatser modificeret inden for de seneste 4 dage, der matcher de konfigurerede organisationer og workflowstatusser.
3. Fylder arbejdskøen med hver matchende indsats (CPR-nummer, indsats-ID, navn og seneste ændringstidspunkt).
4. For hvert element i arbejdskøen slås borgeren op i KMD Nexus og borgerens fulde forløbsvisning hentes.
5. Den specifikke indsats identificeres på forløbet via indsats-ID.
6. Indsatsens leverandør tjekkes mod listen over irrelevante leverandører – elementet springes over, hvis leverandøren mangler eller er på eksklusionslisten.
7. Eksisterende opgavehistorik på indsatsen gennemgås for at undgå dubletter – elementet springes over, hvis der allerede findes en aktiv opgave eller en nyere opgave er oprettet.
8. Der oprettes en opgave af typen "Indsatser til økonomi – voksne" på indsatsen, tildelt "Regnskab BSF", med dags dato som start- og forfaldsdato.
9. Den oprettede opgave registreres i ODK-sporingssystemet.

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø)
- Adgang til **KMD Nexus** (produktion og database)
- Adgang til **Odense SQL Server**
- Excel-filen `Regelsæt.xlsx` placeret i processmappen med organisationer, indsatsstatusser og leverandørfiltre

## Installation

```sh
uv sync
```

## Konfiguration

Credentials registreres i Automation Server:
- `KMD Nexus - produktion`
- `KMD Nexus - database`
- `Odense SQL Server`

| Miljøvariabel | Beskrivelse |
|---|---|
| `ATS_URL` | URL til Automation Server-instansen (standard: `http://localhost:8000`) |
| `ATS_TOKEN` | Bearer-token til autentificering mod Automation Server |
| `ATS_WORKQUEUE_OVERRIDE` | Tilsidesæt arbejdskø-ID (bruges til test) |

## Kørsel

```sh
uv run python main.py --queue   # Fyld arbejdskøen
uv run python main.py           # Behandl arbejdskøen
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Håndtering af arbejdskø og credentials via Automation Server |
| `kmd-nexus-client` | Opslag og opdatering af data i KMD Nexus (borgere, indsatser, opgaver) |
| `nexus-database-client` | Direkte databaseforespørgsler i Nexus efter nyligt ændrede indsatser |
| `odk-tools` | Registrering af afsluttede opgaver i ODK-sporingssystemet |
| `openpyxl` | Læsning af Excel-regelfilen (`Regelsæt.xlsx`) |
| `python-dateutil` | Parsing af ISO-datostrenge fra Nexus API |
| `tzdata` | Tidszonehåndtering for København |

## GDPR og sikkerhed

Robotten behandler CPR-numre på voksne borgere som reference i arbejdskøen og til opslag i KMD Nexus. CPR-numrene opbevares midlertidigt i arbejdskøen på Automation Server og slettes, når elementet er behandlet. Adgang til Automation Server og Nexus bør begrænses til relevante medarbejdere og systemkonti.
