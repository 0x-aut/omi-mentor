import asyncio
from typing import Dict, List, Any

# We want to either create a custom queue or use the builtin queue.

class MentorQueue:  # This class is probably redundant but it will do
  def __init__(self):
    self.queue = asyncio.Queue(maxsize=0) # infinity in size for now
    
  async def putItem(self, item: Dict):
    await self.queue.put(item)
    
  async def getItem(self):
    return await self.queue.get()
  
  async def putItem_NoBlock(self, item: Dict):
    await self.queue.put_nowait(item)
  
  async def shutDownQueue(self):
    await self.queue.shutdown # No need for queuefull since it is theoretically infinite
    