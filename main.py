#Framework imports
from asyncio.queues import QueueShutDown
from fastapi import FastAPI, Body
# from pydantic import BaseModel
from fastapi.requests import Request
from fastapi.middleware.cors import CORSMiddleware

#Python packages imports
from pprint import pprint
import asyncio
import time
from datetime import datetime
# import threading
from typing import List
# from contextlib import asynccontextmanager

#File/Module imports
from data.model import Segment
from prompt.notification import *
from prompt.advice import *
from Logcode import *
from data.constants import *
from data.context import queue_list
from utils.notifications import *
from utils.Buffer import MessageBuffer
#from utils.gettime import get_transcript_on_time
#from data.context import conversations_list, transcript_segment, unclean_context_list
from utils.conversation import Conversations
from utils.OneQueue import MentorQueue


app = FastAPI()

origins = [
  "http://localhost:8000",
  "http://localhost"
]

app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"], #origins
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"]
)

start_time = time.time()
logger.info(f"Application initialized. Start time: {datetime.fromtimestamp(start_time)}")


# logger.info("Starting Mentor notification service")


message_buffer = MessageBuffer()
# logger.info(f"Analysis interval set to {END_OF_CONVERSATION_IN_SECONDS} seconds")
conversations = Conversations()
mentorQueue = MentorQueue()

''''''

'''IMPORTANT NOTE: The thread below hijacks the main thread and stops the app from running. This is not ideal and should be fixed.
FIX: A simple fix would be to ensure it runs in the background AFTER the app has started.
A condition could be after a segment is gotten from the transcript, then the thread starts.
This way, the app can run and the thread can run in the background.
At the moment though we can remove the notifications for now.
'''
# # This starts the reminder check loop in the background, message_buffer MUST be initialized first though
# reminder_thread = threading.Thread(target=reminder_check_loop(message_buffer), daemon=True)
# reminder_thread.start()

# # Silence checker for the request when the app starts
# @app.on_event("startup")
# async def startup_event():
#   asyncio.create_task(conversations.transcript_worker())
#   logger.info("Asyncio streaming data task started")
#   asyncio.sleep(2.0)
  
# @app.on_event("shutdown")
# async def shutdown_event():
#   conversations.stop_count_thread()
#   logger.info("Stopped time thread, ended server lifespan")


### We need to change the way the server collects the trasncript from omi.

main_advice = ""
# pseudo_segment_list = []
queue_shutdown = False

@app.post("/webhook")
async def webhook(session_id: str = Body(...), segments: List[Segment] = Body(..., embed=True)):
  try:
    segment_json = [segment.model_dump(mode="json") for segment in segments]
    logger.info(f"Segments converted to json are: {segment_json}")
    
    # We want to ensure complete text past a certain period of time right?
    # '''
    # Segments converted to json are: 
        # [
        # {'text': 'Testing this, environment see if the', 'speaker': 'SPEAKER_0', 'speaker_id': 0, 'is_user': False, 'person_id': None, 'start': 0.0, 'end': 3.26}, 
        # {'text': 'developer server works.', 'speaker': 'SPEAKER_0', 'speaker_id': 0, 'is_user': False, 'person_id': None, 'start': 4.7, 'end': 5.66}
        # ]
        # 
    #Segments converted to json are: 
        # [
        # {'text': "Yeah. Let's see how it goes.", 'speaker': 'SPEAKER_0', 'speaker_id': 0, 'is_user': False, 'person_id': None, 'start': 7.67, 'end': 9.19}]
    # '''
    # 
    # Since segments come it at differing times we want to block main thread until a certain time has elapsed
    
    
    # So we can store the message in a queue right? a global queue?
    # And the queue is locked after a certain period of time
    
    # check time for each while loop.
    checked_time = time.time() - start_time
    end_time = segment_json[len(segment_json)-1]['end'] - checked_time
    
    if (end_time < 10):
      for segment in segment_json:
        try:
          mentorQueue.putItem_NoBlock(segment)
          logger.info(f"Current queue is: {mentorQueue}")
        except QueueShutDown:
          queue_shutdown = True
          logger.info("Queue has been shut down and will not collect more segments")
          logger.info("Will process segments now")
    else:
      mentorQueue.shutDownQueue() # We want to shut down queue basically.
    
    if queue_shutdown == True:
     logger.info(f"Final queue is: {mentorQueue}")
     
     
      
    # for segment in segment_json:
    #   pseudo_segment_list.append(segment)
    #   await conversations.put_transcript_in_queue(segment)
    
    # # await oneQueue.fill_queue_multiple_items(segment_json)
    # # pseudo_transcript = await asyncio.wait_for(oneQueue.queue.get(), timeout=5) # I think this shoots me no??
    
    # if conversations.rate_limit_count == 0:
    #   conversations.reset_rate_limit()
  
    # # message_id should be generated if it isnt provided # Strangely i doubt this is needed, unless for scaling?
    # if not message_id:
    #   message_id = f"{session_id}_{int(time.time())}"
    #   logger.info(f"Generated message_id: {message_id}")
      
    # logger.info(f"Processing webhook for session_id: {session_id}, message_id: {message_id}, segments count: {len(segments)}, aid: {APP_ID}")
      
    # if not session_id:
    #   logger.error("No session_id provided in request")
    #   return {"message": "No session_id provided"}
    
    # if conversations.interrupt_flag.is_set(): ## No rate limit here
    #   conversations.reset_interrupt_flag()
    #   logger.info(f"AI interrupting: Interrupting the conversation")
    #   logger.info(f"Creating the notification prompt early")
    #   # total_conversation = conversations.join_conversation(convo_list)
    #   notification = create_notification_prompt(conversations.conversation)
    #   logger.info(f"Sending notification prompt template for advice")
    #   advice = get_advice(notification)
    #   if advice:
    #     return {"message": f"{advice}"}
    #   else:
    #     logger.error("An error occured while sending advice")
    
    # if conversations.end_convo_flag.is_set():
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
          
      # if advice:
        # logger.info(f"Advice has been created: {advice}")
    # return {"message": f"{advice}"}
      # else:
      #   logger.error("An error occured while sending advice")
    # else:
    #   pass
        
  except Exception as e:
    logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
    return {"error": "Internal server error"}              
  # return {"message": f"Transcript: {segments}"}

@app.get('/webhook/setup-status')
def setup_status():
  logger.debug("Received setup-status GET request")
  return {"is_setup_completed": True}

@app.get('/status')
def status():
  logger.debug("Received status GET request")
  active_sessions = len(message_buffer.buffers)
  uptime = time.time() - start_time
  logger.info(f"Status check - Active sessions: {active_sessions}, Uptime: {uptime:.2f}s")
  return {
    "active_sessions": active_sessions,
    "uptime": uptime
  }

if __name__ == '__main__':
  import uvicorn
  uvicorn.run(app, host="127.0.0.1", port=8000)