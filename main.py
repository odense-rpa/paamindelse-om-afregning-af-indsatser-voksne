import argparse
import asyncio
import logging
import sys
import os

from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential
from nexus_database_client import NexusDatabaseClient
from kmd_nexus_client import NexusClientManager
from odk_tools.tracking import Tracker
from odk_tools.reporting import Reporter
from process.config import get_excel_mapping, load_excel_mapping
from kmd_nexus_client.tree_helpers import (
    filter_by_path, filter_by_predicate,    
)
from datetime import date, datetime
from dateutil.parser import parse
from zoneinfo import ZoneInfo

nexus: NexusClientManager
nexus_database_client: NexusDatabaseClient
tracker: Tracker
reporter: Reporter

proces_navn = "Påmindelse om afregning af indsatser (voksne)"
logger = logging.getLogger(proces_navn)

async def populate_queue(workqueue: Workqueue):
    regler = get_excel_mapping()
    for organisation in regler["Organisationer"]:
        modificerede_indsatser = nexus_database_client.get_modified_grants_by_organisation_name(organisation_name=organisation,  days_back=4, workflow_states=regler["Status på indsats"])

        for indsats in modificerede_indsatser:
            data = {
                "cpr": indsats["business_key"],
                "indsats_id": indsats["id"],
                "indsats_navn": indsats["name"],
                "sidste_aendring": indsats["last_state_change"].strftime("%d-%m-%Y %H:%M:%S"),
            }
                        
            workqueue.add_item(data=data, reference=f"{indsats['id']}")

async def process_workqueue(workqueue: Workqueue):
    regler = get_excel_mapping()
    for item in workqueue:
        with item:            
            try:
                data = item.data
                indsats = hent_indsats(item_data=data)
                
                if not indsats:
                    return None
                
                white_listed_leverandør = kontroller_leverandør(indsats=indsats, regler=regler)

                if not white_listed_leverandør:
                    continue

                opret_opgave(indsats=indsats, item_data=data)
            except WorkItemError as e:
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))

def hent_indsats(item_data: dict) -> dict|None:
    borger = nexus.borgere.hent_borger(item_data["cpr"])

    if not borger:
        return None

    pathway = nexus.borgere.hent_visning(borger=borger)

    if pathway is None:
        raise ValueError(
            f"Kunne ikke finde -Alt for borger {borger['patientIdentifier']['identifier']}"
        )

    indsats_referencer = nexus.borgere.hent_referencer(visning=pathway)            

    filtrerede_indsats_referencer = filter_by_path(
        indsats_referencer,
        path_pattern="/*/*/Indsatser/basketGrantReference",
        active_pathways_only=False,
    )
    
    indsatser = filter_by_predicate(
        roots=filtrerede_indsats_referencer,
        predicate=lambda x: x["grantId"] == item_data["indsats_id"]
    )

    if not indsatser:
        return None

    indsats = nexus.indsatser.hent_indsats(indsatser[0])

    return indsats

def kontroller_leverandør(indsats: dict, regler: dict) -> bool:
    felt_værdier = nexus.indsatser.hent_indsats_elementer(indsats=indsats)

    if not felt_værdier:
        return False

    if felt_værdier["supplier"]["supplier"]["name"] in regler["Irrelevante leverandører"]:
        return False

    return True

def opret_opgave(indsats: dict, item_data: dict) -> None:
    opgaver = nexus.opgaver.hent_opgave_historik(objekt=indsats)
    indsats_ændring = datetime.strptime(item_data["sidste_aendring"], "%d-%m-%Y %H:%M:%S")
    indsats_ændring = indsats_ændring.replace(tzinfo=ZoneInfo("Europe/Copenhagen"))

    if opgaver is not None:
        for opgave in opgaver:
            opgave_ændring = parse(opgave["lastStateChangeDate"])
            opgave_ændring = opgave_ændring.replace(tzinfo=ZoneInfo("Europe/Copenhagen"))
            
            if opgave["type"]["name"] == "Indsatser til økonomi - voksne":
                if (opgave_ændring > indsats_ændring or
                opgave["workflowState"]["name"] == "Aktiv"):
                    return None
    
    nexus.opgaver.opret_opgave(
            objekt=indsats,
            opgave_type="Indsatser til økonomi - voksne",
            titel="Indsats til økonomi - voksne",
            ansvarlig_organisation="Regnskab BSF",
            start_dato=date.today(),
            forfald_dato=date.today(),            
            beskrivelse="Opgave til registrering af indsats i økonomi-systemet.",
            ansvarlig_medarbejder=None            
        )
    
    tracker.track_task(proces_navn)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO        
    )

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

     # Parse command line arguments
    parser = argparse.ArgumentParser(description=proces_navn)
    parser.add_argument(
        "--excel-file",
        default="./Regler.xlsx",
        help="Path to the Excel file containing mapping data (default: ./Regler.xlsx)",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Populate the queue with test data and exit",
    )
    args = parser.parse_args()

    # Validate Excel file exists
    if not os.path.isfile(args.excel_file):
        raise FileNotFoundError(f"Excel file not found: {args.excel_file}")

    # Load POF mapping data once on startup
    load_excel_mapping(args.excel_file)

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    nexus_database_credential = Credential.get_credential("KMD Nexus - database")
    xflow_credential = Credential.get_credential("Xflow - produktion")
    tracking_credential = Credential.get_credential("Odense SQL Server")
    reporting_credential = Credential.get_credential("RoboA")

    nexus = NexusClientManager(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )    
    
    nexus_database_client = NexusDatabaseClient(
        host = nexus_database_credential.data["hostname"],
        port = nexus_database_credential.data["port"],
        user = nexus_database_credential.username,
        password = nexus_database_credential.password,
        database = nexus_database_credential.data["database_name"],
    )

    tracker = Tracker(
        username=tracking_credential.username, 
        password=tracking_credential.password
    )

    reporter = Reporter(
        username=reporting_credential.username,
        password=reporting_credential.password
    )

    logger = logging.getLogger(__name__)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
