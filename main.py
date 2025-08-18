import asyncio
import logging
import sys
from dotenv import load_dotenv
import os
import datetime

from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential
from momentum_client.manager import MomentumClientManager
from odk_tools.tracking import Tracker

load_dotenv()  # Load environment variables from .env file

tracker: Tracker
momentum: MomentumClientManager
proces_navn = "Markering af tekniske forlængelser"




async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)
    
    # Define filters for fetching citizens. Only 6.6: "sygedagpengemodtager"
    filters = [
        {
            "customFilter": "",
            "fieldName": "targetGroupCode",
            "values": [
                "6.6",
            ]
        },
    ]

    # Initialize Momentum client manager and Borgere client
    try:
        borgere = momentum.borgere.hent_borgere(filters=filters)
        
        # Access the actual list of citizens from the 'data' key
        for borger in borgere['data']:
            expected_reason = ('Der udbetales sygedagpenge ud over begrænsningen i § 24, '
                             'fordi der ikke er modtaget en afgørelse om forlængelse '
                             'eller ophør af anden årsag')
            
            # Safe check for prolongation and reasonName
            prolongation = borger.get('prolongation')
            if not prolongation or prolongation.get('reasonName') != expected_reason:
                continue
            
            # Safe handling of tags
            tags = borger.get('tags', [])
            if tags is None:
                tags = []
            else:
                tags = list(tags)  # Convert iterator to list
            
            # Check if there's any active tag with the matching name
            has_active_tag = False
            for tag in tags:
                if isinstance(tag, dict) and tag.get('title') == "Teknisk forlængelse - sygedagpenge":
                    if tag.get('end') is None:  # This tag is active
                        has_active_tag = True
                        break  # Found an active tag, no need to check more
            
            # Add to queue only if there's NO active tag
            if not has_active_tag:
                # Create a new work item for this citizen
                workqueue.add_item(
                    data={
                        'cpr': borger['cpr'],
                    }, 
                    reference=borger['cpr']
                )
    except Exception as e:
        logger.error(f"Failed to fetch borgere: {e}")
        print(f"Error: {e}")
        return



async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict
 
            try:
                # Process the item here
                borger = momentum.borgere.hent_borger(data['cpr'])
                momentum.borgere.opret_markering(
                    borger=borger,
                    start_dato=datetime.datetime.now().date(),
                    markeringsnavn="Teknisk forlængelse - sygedagpenge"
                )
                tracker.track_task(process_name=proces_navn)

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    workqueue = ats.workqueue()

    # Initialize external systems for automation here..
    tracking_credential = Credential.get_credential("Odense SQL Server")
    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    momentum = MomentumClientManager(
    base_url=os.getenv("BASE_URL"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    api_key=os.getenv("API_KEY"),
    resource=os.getenv("RESOURCE"),
)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
