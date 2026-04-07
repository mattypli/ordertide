# only websocket management - connecting ping pong, reconnections etc
import asyncio 
import websockets 
import json 
# import the data from .env through config.py file that fetched them 
import config 

async def listen_to_trades():
    print(f"connecting to {config.WS_URL}")
    
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

        # loop catching messages from the websocket 
        while True: 
            message = await ws.recv() #json response 
            data = json.loads(message) # convert json to python dictionary 

            print("--- new package ---")
            print(data) # print to console, later to database in postgresql 

if __name__ == "__main__": 
    asyncio.run(listen_to_trades())