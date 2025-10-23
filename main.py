# Framework imports
from fastapi import FastAPI, Request, Body, BackgroundTasks
from pydantic import BaseModel
from fastapi.requests import Request
from fastapi.middleware.cors import CORSMiddleware

# Python packages imports
from pprint import pprint
import asyncio
import time
from datetime import datetime

# import threading
from typing import List, Optional
# from contextlib import asynccontextmanager

# File/Module imports
from data.model import Segment
from prompt.notification import *
from prompt.advice import *
from Logcode import *
from data.constants import *
from utils.notifications import *
from utils.Buffer import MessageBuffer

# from utils.gettime import get_transcript_on_time
# from data.context import conversations_list, transcript_segment, unclean_context_list
from utils.conversation import Conversations
from utils.OneQueue import OneQueue


app = FastAPI()

origins = ["http://localhost:8000", "http://localhost"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


"""
NOTE: We are relying on the example given to us with a lot of tweaks since it is a good entry point.
Further work on this app will slowly move away from the example to a more concrete work.
comments tagged with (from example) are gotten from example
comments tagged with (not example) are novel (Some novel comments aren't tagged too)
comments tagged with (perhaps example) are edited example code
"""


# logger.info("Starting Mentor notification service")


message_buffer = MessageBuffer()
logger.info(f"Analysis interval set to {END_OF_CONVERSATION_IN_SECONDS} seconds")
conversations = Conversations()
oneQueue = OneQueue()

""""""

"""IMPORTANT NOTE: The thread below hijacks the main thread and stops the app from running. This is not ideal and should be fixed.
FIX: A simple fix would be to ensure it runs in the background AFTER the app has started.
A condition could be after a segment is gotten from the transcript, then the thread starts.
This way, the app can run and the thread can run in the background.
At the moment though we can remove the notifications for now.
"""
# # This starts the reminder check loop in the background, message_buffer MUST be initialized first though
# reminder_thread = threading.Thread(target=reminder_check_loop(message_buffer), daemon=True)
# reminder_thread.start()


# Silence checker for the request when the app starts
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(conversations.transcript_worker())
    logger.info("Asyncio streaming data task started")
    asyncio.sleep(2.0)


# @app.on_event("shutdown")
# async def shutdown_event():
#   conversations.stop_count_thread()
#   logger.info("Stopped time thread, ended server lifespan")


### We need to change the way the server collects the trasncript from omi.
"""
The transcripts gotten from the omi server (We will call it server) is:
t1, t2, t3, t4

where t2 = t1 + t1.5 (The transcripts are updated per request)

So what we need to do is to call the last transcript gotten after the end of the server send

"""

main_advice = ""


# def create_advice():
#   logger.info("End of conversation detected")
#   logger.info(f"Creating the notification prompt")
#   # total_conversation = conversations.join_conversation(convo_list)
#   notification = create_notification_prompt(conversations.conversation)
#   logger.info(f"Sending notification prompt template for advice")
#   logger.info("Using the rate limit, reset to use again")
#   conversations.use_rate_limit()
#   advice = get_advice(notification)

#   logger.info("Clearing conversation for future use")
#   conversations.reset_conversations()
#   logger.info("Resetting end conversation flag for future use")
#   conversations.reset_end_convo_flag()

#   if advice:
#     logger.info(f"Advice has been created: {advice}")
#     return {"message": f"{advice}"}
#   else:
#     logger.error("An error occured while sending advice")


pseudo_segment_list = []


@app.post("/webhook")
async def webhook(
    session_id: str = Body(...), segments: List[Segment] = Body(..., embed=True)
):
    logger.info("Recieved webhook POST request")
    try:
        message_id = None

        # print(segments) ## At the moment logs shows the segment being returned in a list of a tuple
        ## Response is like this: [Segment(text="",...)]

        segment_json = [segment.model_dump(mode="json") for segment in segments]
        logger.info(f"Segments converted to json are: {segment_json}")

        for segment in segment_json:
            pseudo_segment_list.append(segment)
            await conversations.put_transcript_in_queue(segment)

        # await oneQueue.fill_queue_multiple_items(segment_json)
        # pseudo_transcript = await asyncio.wait_for(oneQueue.queue.get(), timeout=5) # I think this shoots me no??

        if conversations.rate_limit_count == 0:
            conversations.reset_rate_limit()

        # message_id should be generated if it isnt provided # Strangely i doubt this is needed, unless for scaling?
        if not message_id:
            message_id = f"{session_id}_{int(time.time())}"
            logger.info(f"Generated message_id: {message_id}")

        logger.info(
            f"Processing webhook for session_id: {session_id}, message_id: {message_id}, segments count: {len(segments)}, aid: {APP_ID}"
        )

        if not session_id:
            logger.error("No session_id provided in request")
            return {"message": "No session_id provided"}

        if conversations.interrupt_flag.is_set():  ## No rate limit here
            conversations.reset_interrupt_flag()
            logger.info(f"AI interrupting: Interrupting the conversation")
            logger.info(f"Creating the notification prompt early")
            # total_conversation = conversations.join_conversation(convo_list)
            notification = create_notification_prompt(conversations.conversation)
            logger.info(f"Sending notification prompt template for advice")
            advice = get_advice(notification)
            if advice:
                return {"message": f"{advice}"}
            else:
                logger.error("An error occured while sending advice")

        if conversations.end_convo_flag.is_set():
            logger.info("End of conversation detected")
            logger.info(f"Creating the notification prompt")
            # total_conversation = conversations.join_conversation(convo_list)
            notification = create_notification_prompt(conversations.conversation)
            logger.info(f"Sending notification prompt template for advice")
            logger.info("Using the rate limit, reset to use again")
            conversations.use_rate_limit()
            advice = get_advice(notification)

            logger.info("Clearing conversation for future use")
            conversations.reset_conversations()
            logger.info("Resetting end conversation flag for future use")
            conversations.reset_end_convo_flag()

            if advice:
                logger.info(f"Advice has been created: {advice}")
                return {"message": f"{advice}"}
            else:
                logger.error("An error occured while sending advice")
        else:
            pass

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        return {"error": "Internal server error"}
    # return {"message": f"Transcript: {segments}"}


@app.get("/webhook/setup-status")
def setup_status():
    logger.debug("Received setup-status GET request")
    return {"is_setup_completed": True}


@app.get("/status")
def status():
    logger.debug("Received status GET request") 
    active_sessions = len(message_buffer.buffers)
    uptime = time.time() - start_time
    logger.info(
        f"Status check - Active sessions: {active_sessions}, Uptime: {uptime:.2f}s"
    )
    return {"active_sessions": active_sessions, "uptime": uptime}


# Add start time tracking
start_time = time.time()
logger.info(
    f"Application initialized. Start time: {datetime.fromtimestamp(start_time)}"
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
