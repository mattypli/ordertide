# only websocket management - connecting ping pong, reconnections etc
import asyncio 
import websockets 
import json 
# import the data from .env through config.py file that fetched them 
import config 

class WebsocketManager: 
    def __init__(self):
        self.queue = asyncio.Queue() # create an asynchronous FIFO queue for messages 
        self.stop_event = asyncio.Event() # event to signal when to stop the websocket listener and queue reader / atttributes set() and clear(), initially clear() meaning not set
    
    # helper function to safely unsubscribe from all websocket subscriptions before closing connection 
    async def unsubscribe_all(self, ws): # self - websocket manager, ws - websocket
        #unsubscribes all coin pairs 
        for coin in config.COINS: 
            unsubscribe_message = {
                "method": "unsubscribe",
                "subscription": {
                    "type": "trades",
                    "coin": coin
                }
            }
            await ws.send(json.dumps(unsubscribe_message))
        
        await asyncio.sleep(1) # wait for the unsubscription to be processed
        print("unsubscribed from all coins")
    
    # listens to websocket and puts messages in the queue
    async def websocket_listener(self): 
        
        # main while loop, keeps trying to connect and reconnect and listen to the websocket until the stop event is set 
        while not self.stop_event.is_set():
            try: 
                async with websockets.connect(config.WS_URL) as ws: 
                    # subscribe to the trades for the coins we care about
                    print(f"connected to websocket. listening to coins: {config.COINS}")
                    for coin in config.COINS: 
                        subscribe_message = {
                            "method" : "subscribe",
                            "subscription" : {"type": "trades",
                                            "coin": coin} 
                        }
                        await ws.send(json.dumps(subscribe_message)) 
                    
                    print("waiting for data...") 

                    while not self.stop_event.is_set():
                        # task for recieving messages and task for awaiting stop event 
                        recv_task = asyncio.create_task(ws.recv())
                        stop_task = asyncio.create_task(self.stop_event.wait())

                        # wait until either stop event or message recieved 
                        done, pending = await asyncio.wait(
                            [recv_task, stop_task],
                            return_when=asyncio.FIRST_COMPLETED
                        )

                        if stop_task in done:
                            recv_task.cancel() # cancel the recv task if stop event is set
                            await self.unsubscribe_all(ws) # unsubscribe from all pairs before closing connection
                            break 

                        if recv_task in done:
                            message = recv_task.result() # get the message from the completed recv task
                            await self.queue.put(message) # put the message in the queue for the reader
            
            except websockets.exceptions.ConnectionClosed as e:
                if not self.stop_event.is_set(): # connection closed, not because we wanted to stop 
                    print(f"websocket connection closed: {e}")
                    await asyncio.sleep(5) # wait before trying to reconnect
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"error in websocket connection: {e}")
                    await asyncio.sleep(5) # wait before trying to reconnect

    # consumer of the queue // processes messages and writes to database
    async def queue_reader(self):
        # analysing data from the queue and processing it until either stop event is set or queue is empty 
        while not self.stop_event.is_set() or not self.queue.empty():
            try:
                message = await asyncio.wait_for(self.queue.get(), timeout=1.0) # wait for a message from the queue with a TimeoutError trigger 
                data = json.loads(message)

                #TODO process the data - put in the postgresql database 
                

                self.queue.task_done() # mark the message as processed in the queue
            except asyncio.TimeoutError:
                # not an error, it means that no message came in in last 1 second 
                continue 

    async def stop(self):
        print("stopping websocket manager and unsubscribing from all coins...")
        self.stop_event.set() # signal to stop the websocket listener and queue reader

    async def run(self):# run both producer and consumer concurrently
        await asyncio.gather( 
            self.websocket_listener(),
            self.queue_reader() 
        )

async def main(): 
    manager = WebsocketManager() # create an instance of the WebsocketManager class to manage the websockets 

    manager_task = asyncio.create_task(manager.run()) # run WebsocketManagers class function run() as a task 

    try: 
        while True: 
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nprocess stopped by user")
    finally:
        # after exiting the loop, clean up
        await manager.stop()
        await manager_task

if __name__ == "__main__": 
    asyncio.run(main())