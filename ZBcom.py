#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis
import asyncio 
import websockets
import json


pool = redis.ConnectionPool(host='54.168.51.34', port=6379, decode_responses=True)  ##decode_response=True，写入的键值对中的value为str类型,无则为字节类型

r = redis.Redis(connection_pool=pool)

##ZB.com币对,以后固定从mysql数据库里面读取#
symbols = ['1ST_BTC', '1ST_USDT', 'AE_BTC', 'AE_USDT', 'BAT_BTC', 'BAT_USDT', 'BTC_USDT', 'BTM_BTC', 'BTM_USDT', 'BTS_BTC', 'BTS_USDT', 'CHAT_BTC', 'CHAT_USDT', 'DASH_BTC', 'DASH_USDT', 'DOGE_BTC', 'DOGE_USDT', 'EDO_BTC', 'EDO_USDT', 'ENT_BTC', 'ENT_USDT', 'EOS_BTC', 'EOS_USDT', 'ETC_BTC', 'ETC_USDT', 'ETH_BTC', 'ETH_USDT', 'FUN_BTC', 'FUN_USDT', 'GNT_BTC', 'GNT_USDT', 'HLC_BTC', 'HLC_USDT', 'HOTC_BTC', 'HOTC_USDT', 'HPY_BTC', 'HPY_USDT', 'ICX_BTC', 'ICX_USDT', 'INK_BTC', 'INK_USDT', 'KAN_BTC', 'KAN_USDT', 'KNC_BTC', 'KNC_USDT', 'LBTC_BTC', 'LBTC_USDT', 'LTC_BTC', 'LTC_USDT', 'MANA_BTC', 'MANA_USDT', 'MCO_BTC', 'MCO_USDT', 'MTL_BTC', 'MTL_USDT', 'NEO_BTC', 'NEO_USDT', 'OMG_BTC', 'OMG_USDT', 'QTUM_BTC', 'QTUM_USDT', 'QUN_BTC', 'QUN_USDT', 'SAFE_BTC', 'SAFE_USDT', 'SLT_BTC', 'SLT_USDT', 'SNT_BTC', 'SNT_USDT', 'SUB_BTC', 'SUB_USDT', 'TOPC_BTC', 'TOPC_USDT', 'TRUE_BTC', 'TRUE_USDT', 'TV_BTC', 'TV_USDT', 'UBTC_BTC', 'UBTC_USDT', 'XEM_BTC', 'XEM_USDT', 'XLM_BTC', 'XLM_USDT', 'XRP_BTC', 'XRP_USDT', 'XUC_BTC', 'XWC_BTC', 'XWC_USDT', 'ZB_BTC', 'ZB_USDT', 'ZRX_BTC', 'ZRX_USDT']

##一组订阅事件list## 
def produce_json_data_list(symbols): 
    json_data_list = []
    for symbol in symbols:
        channel = symbol.replace('_','').lower() + '_depth'
        json_dict = {}
        json_dict['event'] = 'addChannel'
        json_dict['channel'] = channel
        json_data_list.append(json.dumps(json_dict))
    return json_data_list


def on_message(ws, message):
    exchange = 'ZBcom'
    message = json.loads(message)  
    symbol =  message['channel'].split('_')[0].upper() 
    asks = message['asks'] 
    bids = message['bids'] 
 
    value = {'bids':bids,'asks':asks,'source':exchange}
    value_final = json.dumps(value)
    print('putting ZBcom......')
    r.hset(symbol,exchange,value_final)


async def sendInitMesg(ws):
    event_list = produce_json_data_list(symbols)
    for event in event_list:
        await ws.send(event) 

async def handleMessage(ws,URL):

    while True:
        try:
            greeting = await ws.recv()
            on_message(None,greeting)
        
        except websockets.exceptions.ConnectionClosed:
            print('connect ws error,retry...')
            ws =  await getConnet(URL)

async def getConnet(URL):

    while True:
        try:
            ws = await websockets.connect(URL) 
            await sendInitMesg(ws)
            break
        except Exception as e:
            print(e)
    #print(ws)
    return ws 


async def main(URL):
    #建立连接
    ws = await getConnet(URL) 
	
    await handleMessage(ws,URL)



if __name__ == "__main__":

    URL = 'wss://api.zb.cn:9999/websocket'
    loop = asyncio.get_event_loop()
    
    loop.run_until_complete(main(URL))

    


 
