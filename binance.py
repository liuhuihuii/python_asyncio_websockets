#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis
import asyncio
import websockets
import json


pool = redis.ConnectionPool(host='54.168.51.34', port=6379, decode_responses=True)  ##decode_response=True，写入的键值对中的value为str类型,无则为字节类型

r = redis.Redis(connection_pool=pool)

##处理接收到的数据######
def on_message(ws, message):
    message = json.loads(message)
    exchange = 'Binance'
    symbol = message['stream'].split('@')[0].upper()
    ##下面寻找value####
    data = message['data']
    bids_raw = data['bids']
    bids_after = list()

    for bid in bids_raw:
        item = list()
        item.append(float(bid[0]))    ##价格
        item.append(float(bid[1]))    ##数量
        bids_after.append(item)

    asks_raw = data['asks']
    asks_after = list()

    for ask in asks_raw:
        item = list()
        item.append(float(ask[0]))   ##价格
        item.append(float(ask[1]))   ##数量
        asks_after.append(item)

    value = {'bids':bids_after,'asks':asks_after,'source':exchange}
    value_final = json.dumps(value)

    print('putting Binance.....')
    ###存储按照hashset结构,symbol(大写)+exchange+value值的方式#####
    r.hset(symbol,exchange,value_final)



###生成combined Streams完整api url###
def generate_streams_path(symbol_list):
    streams = []
    for symbol in symbol_list:
        stream = symbol + '@depth20'    ##设置订阅深度 5,10，20
        streams.append(stream)
    stream_path = 'streams={}'.format('/'.join(streams))
    api_url = "wss://stream.binance.com:9443/stream?" + stream_path
    return api_url

async def handleMessage(ws,URL):

    while True:
        try:
            greeting = await ws.recv()

            on_message(None,greeting)

        except websockets.exceptions.ConnectionClosed:
            ws =  await getConnet(URL)

async def getConnet(URL):

    while True:
        try:
            ws = await websockets.connect(URL) 
            break
        except:
            pass
    return ws 


async def main(URL):
   
    ws = await getConnet(URL) 

    await handleMessage(ws,URL)



if __name__ == "__main__":
    threading_list=[]

    symbol_1 = ['adabtc','adaeth','adausdt','adxbtc','adxeth','aebtc','aeeth','agibtc','agieth','aionbtc'
                ,'aioneth','ambbtc','ambeth','appcbtc','appceth','ardrbtc','ardreth','arnbtc','arneth','astbtc'
                ,'asteth','batbtc','bateth','bccbtc','bcceth']

    symbol_2 = ['bccusdt','bcptbtc','bcpteth','blzbtc','blzeth','bnbbtc','bnbeth','bnbusdt','bntbtc','bnteth'
                ,'bqxbtc','bqxeth','brdbtc','brdeth','btcusdt','btgbtc','btgeth','btsbtc','btseth','cdtbtc'
                ,'cdteth','chatbtc','chateth','cloakbtc','cloaketh']

    symbol_3 = ['cmtbtc','cmteth','cndbtc','cndeth','cvcbtc','cvceth','dashbtc','dasheth','databtc','dataeth'
                ,'dentbtc','denteth','dgdbtc','dgdeth','dltbtc','dlteth','dntbtc','dnteth','dockbtc','docketh'
                ,'edobtc','edoeth','elfbtc','elfeth','engbtc']


    symbol_4 = ['engeth','enjbtc','enjeth','etcbtc','etceth','etcusdt','ethbtc','ethusdt','evxbtc','evxeth'
                ,'fuelbtc','fueleth','funbtc','funeth','gntbtc','gnteth','grsbtc','grseth','gtobtc','gtoeth'
                ,'gvtbtc','gvteth','gxsbtc','gxseth','hotbtc']



    symbol_5 = ['hoteth','icnbtc','icneth','insbtc','inseth','iostbtc','iosteth','iotabtc','iotaeth','iotausdt'
                ,'kmdbtc','kmdeth','kncbtc','knceth','lendbtc','lendeth','linkbtc','linketh','loombtc','loometh'
                ,'lrcbtc','lrceth','lskbtc','lsketh','ltcbtc']



    symbol_6 = ['ltceth','ltcusdt','lunbtc','luneth','manabtc','manaeth','mcobtc','mcoeth','mdabtc','mdaeth'
                ,'mftbtc','mfteth','modbtc','modeth','mthbtc','mtheth','mtlbtc','mtleth','nanobtc','nanoeth'
                ,'nasbtc','naseth','navbtc','naveth','ncashbtc']         

    
    symbol_7 = ['ncasheth','neblbtc','nebleth','npxsbtc','npxseth','nulsbtc','nulseth','nulsusdt','nxsbtc','nxseth'
                ,'oaxbtc','oaxeth','omgbtc','omgeth','ontbtc','onteth','ontusdt','ostbtc','osteth','pivxbtc'
                ,'pivxeth','poabtc','poaeth','poebtc','poeeth']
  

    symbol_8 = ['polybtc','powrbtc','powreth','pptbtc','ppteth','qkcbtc','qkceth','qspbtc','qspeth','qtumbtc'
                ,'qtumeth','qtumusdt','rcnbtc','rcneth','rdnbtc','rdneth','repbtc','repeth','reqbtc','reqeth'
                ,'rlcbtc','rlceth','saltbtc','salteth','scbtc']

    symbol_9 = ['sceth','skybtc','skyeth','snglsbtc','snglseth','snmbtc','snmeth','sntbtc','snteth','storjbtc'
                ,'storjeth','stormbtc','stormeth','stratbtc','strateth','subbtc','subeth','sysbtc','syseth','thetabtc'
                ,'thetaeth','tnbbtc','tnbeth','tntbtc','tnteth']

    symbol_10 = ['trxbtc','trxeth','trxusdt','tusdbtc','tusdeth','tusdusdt','vetbtc','veteth','vetusdt','viabtc'
                 ,'viaeth','vibbtc','vibeth','vibebtc','vibeeth','wabibtc','wabieth','wanbtc','waneth','wavesbtc'
                 ,'waveseth','wingsbtc','wingseth','wprbtc','wpreth']

    symbol_11 = ['wtcbtc','wtceth','xembtc','xemeth','xlmbtc','xlmeth','xlmusdt','xmrbtc','xmreth','xrpbtc',
                 'xrpeth','xrpusdt','xvgbtc','xvgeth','xzcbtc','xzceth','zecbtc','zeceth','zenbtc','zeneth'
                 ,'zilbtc','zileth','zrxbtc','zrxeth']

 
    
    symbols = symbol_1+symbol_2+symbol_3+symbol_4+symbol_5+symbol_6+symbol_7+symbol_8+symbol_9+symbol_10+symbol_11
     
    
    url = generate_streams_path(symbols)

    loop = asyncio.get_event_loop()
    
    loop.run_until_complete(main(url))

    


 
