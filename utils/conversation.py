## Imported inbuilt python modules
import asyncio
import time
from typing import Optional
import threading

## Imported python packages
from utils.OneQueue import MentorQueue
from Logcode import *
from data.constants import *
from data.context import *
from prompt.client import client
from utils.Buffer import MessageBuffer


class Conversations:
  """Conversation class to save conversations for the AI as context without outrightly using a db
  It works by continously storing the trasncripts inside a list and checking the server requests
  If a delay is noticed passed the silence time then it simply saves the data it has gotten
  It then uses the data as context for a message for the AI.
  
  We want to ignore all transcripts and get only the last response??
  
  Perhaps it should also feature an interruption method? to get the context of an argument and interrupt where necessary
  """
  def __init__(self, silence_threshold=END_OF_CONVERSATION_IN_SECONDS):
    self.onequeue = MentorQueue()
    # self.onequeue_queue = self.onequeue.queue
    self.last_request_time = time.time()
    self.conversations = conversations_list
    self.silence_time = silence_threshold
    self.conversation = ""
    self.notification_sent = False
    self.lock = asyncio.Lock() # For thread-safe updates
    self.current_count = 0
    self.running = False # Thread flag for control
    self.count_thread = None
    self.silence = False
    self.conversation_queue = asyncio.Queue(maxsize=1)
    self.start_queue = False
    self.end_convo_flag = asyncio.Event() # Event to signal end of conversation
    self.interrupt_flag = asyncio.Event() # Event to signal interruption
    self.rate_limit_count = 1
    self.pseudo_conversations_list = []
    
  def update(self, transcript_segment: str):
    logger.info(f"Updating the conversation for better context")
    self.conversations.append(transcript_segment)
    self.last_request_time = time.time()
    self.notification_sent = False
    return self.conversations
    
  def should_interrupt(self) -> bool: # We are removing async from the function name for now
    logger.info(f"Starting interrupt function")
    try:
      logger.debug(f"Sending request to groq API")
      system_prompt = """
You are a conversation specialist. 
Your job is to look at the conversation the user gives and check if it needs an interruption.
Have an added focus on whether the conversation sounds like an argument. (Arguments should be priority so most of the time you should interrupt an argument)
If the conversation is deemed necessary to be interrupted then ONLY respond with the word: INTERRUPT
If the conversation is not deemed necessary to be interrupted then simply respond with the word: NOINTERRUPT

Example of situations that could require interruption:
- When the user is getting a fact wrong, it warrants interruption to correct the user
- When the user is in an argument or a heated argument, IT ABSOLUTELY WARRANTS INTERRUPTION. It is important to understand what the argument is about, why it is ocurring and how it can be resolved. DO NOT PICK A SIDE UNLESS ABSOLUTELY SURE OF WHO IS AT FAULT
- When a conversation by the user seems like it needs urgent help it warrants interruption.

You are:
- A mentor where needed.
- A friend where needed.
- A confidant where needed.

Using the three situational guidelines above you can guess other possible situations that require interruption and others that don't require interruption.
This is the conversation for you to go through to make your assessment: {transcript_segment}
Your OUTPUT text should either be INTERRUPT or NOINTERRUPT
IT MUST BE IN CAPS""".format(transcript_segment=self.conversation)

      response = client.chat.completions.create(
        model = AI_MODEL,
        messages = [
          {
            "role": "system",
            "content": system_prompt
          },
          {
            "role": "user",
            "content": f"Please tell me if it's necessary to interrupt this discussion or not: {self.conversation}"
          }
        ],
        max_tokens=150,
        temperature=0.3
      )
      logger.info("Getting interruption status from conversation")
      response_text = response.choices[0].message.content
      logger.debug(f"Response from groq API: {response_text}")
      logger.info(f"Successfully gotten interruption status.")
      if response_text == "INTERRUPT":
        return True
      elif response_text == "NOINTERRUPT":
        return False
      else:
        logger.error("INCORRECT OUTPUT GOTTEN")
        return False
    except Exception as e:
      logger.error(f"An error occured: {str(e)}", exc_info=True)
      return False
    
    
  async def check_silence(self):
    logger.info("Checking for silence in the conversation")
    while True:
      logger.info("Silence checking loop")
      async with self.lock:
        current_time = time.time()
        time_since_last_transcript = current_time - self.last_request_time
        if time_since_last_transcript >= self.silence_threshold and self.notification_sent == False:
          logging.info("Silence detected - Conversation ended. Sending notification")
          self.notification = True
          return True
        elif time_since_last_transcript < self.silence_threshold and not self.notification_sent:
          ## We could log here but we dont want spam every second
          logger.info("Silence not detected - Conversation ongoing")
          return False
        elif time_since_last_transcript >= self.silence_threshold and not self.notification_sent:
          logging.info("Silence detected - Conversation ended. Sending notification again")
          self.notification_sent = False
          return True
      await asyncio.sleep(1.0)
      
  def join_conversation(self, conversation_list: list) -> str:
    """Function to join the conversation list into a single string
    """
    convo = ""
    for conversation in conversation_list:
      convo += conversation + " "
    
    self.conversation = convo.strip()
    return self.conversation
  
  def join_conversation_from_transcript(self, conversation_list: list) -> str:
    """Function to join the conversation list from a transcript into a single string
    """
    convo = ""
    for conversation in conversation_list:
      convo += conversation['text'] + " "
    
    self.conversation = convo.strip()
    return self.conversation
  
  ## Some functions here aren't necessary to be honest, we could simply hardcode them, but i wanna use them as functions
  
  def reset_interrupt_flag(self):
    """Clear the interrupt flag
    """
    self.interrupt_flag.clear()
    
  def reset_end_convo_flag(self):
    """Clear the end conversation flag
    """
    self.end_convo_flag.clear()
    
  def reset_conversations(self):
    """Resets the conversations to its empty state.
    """
    conversations_list = []
    self.conversations = conversations_list
    self.conversation = ""
    
  def reset_rate_limit(self):
    """Resets the rate limit from 0 to default
    """
    self.rate_limit_count = 1
  
  def use_rate_limit(self):
    """Uses the rate limit from default to new number
    """
    self.rate_limit_count = 0
  
  async def transcript_worker(self):
    """Add the transcript to the asyncio queue easily.
    """
    # We can make it so it starts running when the queue changes (something enters it)
    # And it will stop when the queue is flushed, this will save network resources yes?
    while True:
      ## Rework necessary
      try:
        pseudo_conversations = await asyncio.wait_for(self.conversation_queue.get(), timeout=self.silence_time+3)
        
        self.pseudo_conversations_list.append(pseudo_conversations)
        logger.info(f"Sent convo to pseudo conversations: {self.pseudo_conversations_list}")
        
        logger.info(f"Received transcript segment from queue: {self.conversations}")
        
        joined_conversation = self.join_conversation_from_transcript(self.conversations)
        logger.info(f"Joined conversation to check for interruption")
        if self.should_interrupt() == True: ## Is await needed here? yes
          logger.info("Interruption needed")
          self.interrupt_flag.set()
          ## We are going to straight up code a bug here to allow unlimited rate limits when we are interrupting (Will probably be fixed later depending on how it goes)
          ## Any race condition here? doubt it
      
      except asyncio.TimeoutError:
        logger.info("Silence detected in the conversation")
        # self.conversations = self.conversations_wait
        logger.info("Edited conversations list")
        logger.info(f"Conversations list: {self.conversations}")
        self.join_conversation_from_transcript(self.conversations)
        logger.info(f"Created the full conversation: {self.conversation}")
        
        if self.conversation != "":
          if self.rate_limit_count == 1:
            self.end_convo_flag.set() # Signal the end of conversation
            await self.flush_queue()
          elif self.rate_limit == 0:
            logger.info("Rate limit reached, waiting for next legitimate request")
            #self.rate_limit_count = 1
            await self.flush_queue()
            self.reset_conversations()
            self.reset_rate_limit()
        else:
          if self.rate_limit_count == 0:
            self.reset_rate_limit()
          logger.info("There is nothing to do for now")
        

  async def put_transcript_in_queue(self, transcript_segment):
    """Puts the transcript in the ayncio queue
    """
    logger.info("Adding the transcript segment to queue")
    if self.conversation_queue.qsize() > 0:
      await self.conversation_queue.get()
      logger.info("Removed previous item from queue")
    else:
      logger.info("Queue is empty")
    await self.conversation_queue.put(transcript_segment)
    logger.info(f"Conversation queue: {self.conversation_queue}")
    logger.info("Successfully added segment to queue")

  async def flush_queue(self):
    """This function flushes the queue
    """
    while not self.conversation_queue.empty():
        await self.conversation_queue.get()
      