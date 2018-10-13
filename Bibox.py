import asyncio 
import json
import websockets
import redis   
import base64  
import gzip
import ast 

pool = redis.ConnectionPool(host='54.168.51.34', port=6379, decode_responses=True)  ##decode_response=True，写入的键值对中的value为str类型,无则为字节类型
r = redis.Redis(connection_pool=pool)

symbols = ['ABT_BTC', 'ABT_ETH', 'AC3_BTC', 'AC3_ETH', 'ACAT_BTC', 'ACAT_ETH', 'AIDOC_BTC', 'AIDOC_ETH', 'AMM_BTC', 'AMM_ETH', 'AT_BTC', 'AT_ETH', 'BCH_BTC', 'BCH_ETH', 'BCH_USDT', 'BCV_BTC', 'BCV_ETH', 'BIX_BTC', 'BIX_ETH', 'BIX_USDT', 'BLT_BTC', 'BLT_ETH', 'BOE_BTC', 'BOE_ETH', 'BOT_BTC', 'BOT_ETH', 'BTC_USDT', 'BTO_BTC', 'BTO_ETH', 'BZNT_BTC', 'BZNT_ETH', 'C20_BTC', 'C20_ETH', 'CAG_BTC', 'CAG_ETH', 'CAR_BTC', 'CAR_ETH', 'CAT_BTC', 'CAT_ETH', 'CMT_BTC', 'CMT_ETH', 'CPC_BTC', 'CPC_ETH', 'CZR_BTC', 'CZR_ETH', 'DASH_BTC', 'DASH_ETH', 'DASH_USDT', 'DCC_BTC', 'DCC_ETH', 'DTA_BTC', 'DTA_ETH', 'DXT_BTC', 'DXT_ETH', 'ELF_BTC', 'ELF_ETH', 'ETC_BTC', 'ETC_ETH', 'ETC_USDT', 'ETH_BTC', 'ETH_USDT', 'FSN_BTC', 'FSN_ETH', 'FSN_USDT', 'GNX_BTC', 'GNX_ETH', 'GTC_BTC', 'GTC_ETH', 'GTO_BTC', 'GTO_ETH', 'HDAC_BTC', 'HDAC_ETH', 'HER_BTC', 'HER_ETH', 'HPB_BTC', 'HPB_ETH', 'HPB_USDT', 'HT_BTC', 'HT_ETH', 'HT_USDT', 'INSTAR_BTC', 'INSTAR_BTC', 'IPSX_BTC', 'IPSX_ETH', 'ITC_BTC', 'ITC_ETH', 'JNT_BTC', 'JNT_ETH', 'KICK_BTC', 'KICK_ETH', 'LBA_BTC', 'LBA_ETH', 'LBA_USDT', 'LGO_BTC', 'LGO_ETH', 'LKN_BTC', 'LKN_ETH', 'LTC_BTC', 'LTC_ETH', 'LTC_USDT', 'MANA_BTC', 'MANA_ETH', 'MED_BTC', 'MED_ETH', 'MKR_BTC', 'MKR_ETH', 'MOT_BTC', 'MOT_ETH', 'MT_BTC', 'MT_ETH', 'MTC_BTC', 'MTC_ETH', 'MTC_USDT', 'NEO_BTC', 'NEO_ETH', 'NEO_USDT', 'NPER_BTC', 'NPER_ETH', 'PAI_BTC', 'PAI_ETH', 'POA_BTC', 'POA_ETH', 'PRA_BTC', 'PRA_ETH', 'QTUM_BTC', 'QTUM_ETH', 'QTUM_USDT', 'RDN_BTC', 'RDN_ETH', 'RED_BTC', 'RED_ETH', 'RDN_BTC', 'RDN_ETH', 'RFR_ETH', 'RTE_BTC', 'RTE_BTC', 'SNOV_BTC', 'SNOV_ETH', 'SXUT_BTC', 'SXUT_ETH', 'TNB_BTC', 'TNB_ETH', 'TNC_BTC', 'TNC_ETH', 'UPP_BTC', 'UPP_ETH', 'UPP_USDT', 'UUU_BTC', 'UUU_ETH', 'WAX_BTC', 'WAX_ETH']
 

##包装订阅topic####
def wrapSendEvent(symbols):
    evet_list = []
    for symbol in symbols:
        dict1 ={}
        dict1['event'] = "addChannel"
        dict1['channel'] = "bibox_sub_spot_"+ symbol + "_depth"
        evet_list.append(json.dumps(dict1))
    return evet_list


def on_message(ws,message):
    msg = ast.literal_eval(message)
    symbol = msg[0]['channel'].split('_')[3] + msg[0]['channel'].split('_')[4]
    data = msg[0]['data']
    ##base64解码##
    decode_data =base64.b64decode(data)
    ##gzip解压##
    msg_decompressed = gzip.decompress(decode_data).decode('utf-8')
    msg_dict = json.loads(msg_decompressed)

    asks = msg_dict['asks']
    asks_after = []
    for ask in asks:
        item = list()
        item.append(float(ask['price']))
        item.append(float(ask['volume']))
        asks_after.append(item)

    bids = msg_dict['bids'] 
    bids_after = []
    for bid in bids:
        item = list()
        item.append(float(bid['price']))
        item.append(float(bid['volume']))
        bids_after.append(item)
    exchange = 'Bibox'

    value = {'bids':bids_after,'asks':asks_after,'source':exchange}
    value_final = json.dumps(value)
    r.hset(symbol,exchange,value_final)
    print('putting Bibox......')

async def sendInitMesg(ws):
    msg_list = wrapSendEvent(symbols)
    for msg in msg_list:
        await ws.send(msg)

async def getConnet(URL):

    while True:
        try:
            ws = await websockets.connect(URL) 
            #向sever端发送数据#
            await sendInitMesg(ws)
            break
        except Exception as e:
            print(e)
    return ws 


async def handleMessage(ws,URL):

    while True:
        try:
            greeting = await ws.recv()

            on_message(None,greeting)
        except websockets.exceptions.ConnectionClosed:
            print('connect ws error,retry...')
            ws =  await getConnet(URL)

async def main(URL):
    #建立连接
    ws = await getConnet(URL) 
	
    await handleMessage(ws,URL)



if __name__ == "__main__":

    URL = 'wss://push.bibox.com/'
    loop = asyncio.get_event_loop()
    
    loop.run_until_complete(main(URL))

