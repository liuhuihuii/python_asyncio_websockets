#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis
import asyncio
import ast 
import websockets
import json


pool = redis.ConnectionPool(host='54.168.51.34', port=6379, decode_responses=True)  ##decode_response=True，写入的键值对中的value为str类型,无则为字节类型

r = redis.Redis(connection_pool=pool)

symbols =['1ST_BTC', '1ST_ETH', '1ST_USDT', 'ABT_BTC', 'ABT_ETH', 'ABT_USDT', 'ACE_BTC', 'ACE_ETH', 'ACE_USDT', 'ACT_BTC', 'ACT_ETH', 'ACT_USDT', 'ADA_BTC', 'ADA_ETH', 'ADA_USDT', 'AE_BTC', 'AE_ETH', 'AE_USDT', 'AIDOC_BTC', 'AIDOC_ETH', 'AIDOC_USDT', 'AMM_BTC', 'AMM_ETH', 'AMM_USDT', 'ARDR_BTC', 'ARK_BTC', 'ARK_ETH', 'ARK_USDT', 'AST_BTC', 'AST_ETH', 'AST_USDT', 'ATL_ETH', 'ATL_USDT', 'AUTO_BTC', 'AUTO_ETH', 'AUTO_USDT', 'AVT_BTC', 'AVT_ETH', 'AVT_USDT', 'BCD_BTC', 'BCD_USDT', 'BCH_BTC', 'BCH_ETH', 'BCH_USDT', 'BKX_BTC', 'BKX_ETH', 'BKX_USDT', 'BNT_BTC', 'BNT_ETH', 'BNT_USDT', 'BRD_ETH', 'BRD_USDT', 'BTC_USDT', 'BTG_BTC', 'BTG_USDT', 'BTM_BTC', 'BTM_ETH', 'BTM_USDT', 'CAG_BTC', 'CAG_ETH', 'CAG_USDT', 'CAI_BTC', 'CAI_ETH', 'CAI_USDT', 'CBT_BTC', 'CBT_ETH', 'CBT_USDT', 'CHAT_BTC', 'CHAT_ETH', 'CHAT_USDT', 'CIT_BTC', 'CIT_ETH', 'CMT_BTC', 'CMT_ETH', 'CMT_USDT', 'CTR_BTC', 'CTR_ETH', 'CTR_USDT', 'CTXC_BTC', 'CTXC_ETH', 'CTXC_USDT', 'CVC_BTC', 'CVC_ETH', 'CVC_USDT', 'CVT_BTC', 'CVT_ETH', 'CVT_USDT', 'DADI_BTC', 'DADI_ETH', 'DADI_USDT', 'DASH_BTC', 'DASH_ETH', 'DASH_USDT', 'DAT_BTC', 'DAT_ETH', 'DAT_USDT', 'DCR_BTC', 'DCR_ETH', 'DCR_USDT', 'DENT_BTC', 'DENT_ETH', 'DENT_USDT', 'DGD_BTC', 'DGD_ETH', 'DGD_USDT', 'DNA_BTC', 'DNA_ETH', 'DNA_USDT', 'DNT_BTC', 'DNT_ETH', 'DNT_USDT', 'DPY_BTC', 'DPY_ETH', 'DPY_USDT', 'EDO_BTC', 'EDO_ETH', 'EDO_USDT', 'EGT_BTC', 'EGT_ETH', 'EGT_USDT', 'ELF_BTC', 'ELF_ETH', 'ELF_USDT', 'ENG_BTC', 'ENG_ETH', 'ENG_USDT', 'ENJ_BTC', 'ENJ_ETH', 'ENJ_USDT', 'EOS_BTC', 'EOS_ETH', 'EOS_USDT', 'ETC_BTC', 'ETC_ETH', 'ETC_USDT', 'ETH_BTC', 'ETH_USDT', 'EVX_BTC', 'EVX_ETH', 'EVX_USDT', 'FUN_BTC', 'FUN_ETH', 'FUN_USDT', 'GNT_BTC', 'GNT_ETH', 'GNT_USDT', 'GNX_BTC', 'GNX_ETH', 'GNX_USDT', 'GSC_BTC', 'GSC_ETH', 'GSC_USDT', 'GTC_BTC', 'GTC_ETH', 'GTC_USDT', 'GTO_BTC', 'GTO_ETH', 'GTO_USDT', 'HMC_BTC', 'HMC_ETH', 'HMC_USDT', 'HOT_BTC', 'HOT_ETH', 'HOT_USDT', 'HPB_BTC', 'HPB_ETH', 'HPB_USDT', 'HYC_BTC', 'HYC_ETH', 'HYC_USDT', 'ICN_BTC', 'ICN_ETH', 'ICN_USDT', 'INS_BTC', 'INS_ETH', 'INS_USDT', 'INSUR_BTC', 'INSUR_ETH', 'INSUR_USDT', 'INT_BTC', 'INT_ETH', 'INT_USDT', 'IOST_BTC', 'IOST_ETH', 'IOST_USDT', 'IPC_BTC', 'IPC_ETH', 'IPC_USDT', 'ITC_BTC', 'ITC_ETH', 'ITC_USDT', 'KAN_BTC', 'KAN_ETH', 'KAN_USDT', 'KNC_BTC', 'KNC_ETH', 'KNC_USDT', 'LA_BTC', 'LA_ETH', 'LA_USDT', 'LBA_BTC', 'LBA_ETH', 'LBA_USDT', 'LEND_BTC', 'LEND_ETH', 'LEND_USDT', 'LET_BTC', 'LET_ETH', 'LET_USDT', 'LEV_BTC', 'LEV_ETH', 'LEV_USDT', 'LIGHT_BTC', 'LIGHT_ETH', 'LIGHT_USDT', 'LINK_BTC', 'LINK_ETH', 'LINK_USDT', 'LRC_BTC', 'LRC_ETH', 'LRC_USDT', 'LSK_BTC', 'LSK_ETH', 'LSK_USDT', 'LTC_BTC', 'LTC_ETH', 'LTC_USDT', 'MAG_BTC', 'MAG_ETH', 'MAG_USDT', 'MANA_BTC', 'MANA_ETH', 'MANA_USDT', 'MCO_BTC', 'MCO_ETH', 'MCO_USDT', 'MDA_BTC', 'MDA_ETH', 'MDA_USDT', 'MDT_BTC', 'MDT_ETH', 'MDT_USDT', 'MKR_BTC', 'MKR_ETH', 'MKR_USDT', 'MOF_BTC', 'MOF_ETH', 'MOF_USDT', 'MOT_BTC', 'MOT_ETH', 'MOT_USDT', 'MTL_BTC', 'MTL_ETH', 'MTL_USDT', 'MVP_BTC', 'MVP_ETH', 'MVP_USDT', 'NANO_BTC', 'NANO_ETH', 'NANO_USDT', 'NGC_BTC', 'NGC_ETH', 'NGC_USDT', 'NULS_BTC', 'NULS_ETH', 'NULS_USDT', 'NXT_BTC', 'OAX_BTC', 'OAX_ETH', 'OAX_USDT', 'OF_BTC', 'OF_ETH', 'OF_USDT', 'OMG_BTC', 'OMG_ETH', 'OMG_USDT', 'ORS_BTC', 'ORS_ETH', 'ORS_USDT', 'OST_ETH', 'OST_USDT', 'PAY_BTC', 'PAY_ETH', 'PAY_USDT', 'POE_BTC', 'POE_ETH', 'POE_USDT', 'PPT_BTC', 'PPT_ETH', 'PPT_USDT', 'PRA_BTC', 'PRA_ETH', 'PRA_USDT', 'PST_BTC', 'PST_ETH', 'PST_USDT', 'QTUM_BTC', 'QTUM_ETH', 'QTUM_USDT', 'QUN_BTC', 'QUN_ETH', 'QUN_USDT', 'QVT_BTC', 'QVT_ETH', 'QVT_USDT', 'R_BTC', 'R_ETH', 'R_USDT', 'RCN_BTC', 'RCN_ETH', 'RCN_USDT', 'RCT_BTC', 'RCT_ETH', 'RCT_USDT', 'RDN_BTC', 'RDN_ETH', 'RDN_USDT', 'READ_BTC', 'READ_ETH', 'READ_USDT', 'REF_BTC', 'REF_ETH', 'REF_USDT', 'REN_BTC', 'REN_ETH', 'REN_USDT', 'REQ_BTC', 'REQ_ETH', 'REQ_USDT', 'RFR_BTC', 'RFR_ETH', 'RFR_USDT', 'RNT_BTC', 'RNT_ETH', 'RNT_USDT', 'SALT_BTC', 'SALT_ETH', 'SALT_USDT', 'SAN_BTC', 'SAN_ETH', 'SAN_USDT', 'SC_BTC', 'SC_ETH', 'SC_USDT', 'SDA_BTC', 'SDA_ETH', 'SHOW_BTC', 'SHOW_ETH', 'SHOW_USDT', 'SNC_BTC', 'SNC_ETH', 'SNC_USDT', 'SNGLS_BTC', 'SNGLS_ETH', 'SNGLS_USDT', 'SNT_BTC', 'SNT_ETH', 'SNT_USDT', 'SOC_BTC', 'SOC_ETH', 'SOC_USDT', 'SPF_BTC', 'SPF_ETH', 'SPF_USDT', 'SSC_BTC', 'SSC_ETH', 'SSC_USDT', 'STC_BTC', 'STC_ETH', 'STC_USDT', 'STORJ_BTC', 'STORJ_ETH', 'STORJ_USDT', 'SUB_BTC', 'SUB_ETH', 'SUB_USDT', 'SWFTC_BTC', 'SWFTC_ETH', 'SWFTC_USDT', 'TCT_BTC', 'TCT_ETH', 'TCT_USDT', 'THETA_BTC', 'THETA_ETH', 'THETA_USDT', 'TNB_BTC', 'TNB_ETH', 'TNB_USDT', 'TOPC_BTC', 'TOPC_ETH', 'TOPC_USDT', 'TRIO_BTC', 'TRIO_ETH', 'TRIO_USDT', 'TRUE_BTC', 'TRUE_ETH', 'TRUE_USDT', 'TRX_BTC', 'TRX_ETH', 'TRX_USDT', 'UGC_BTC', 'UGC_ETH', 'UGC_USDT', 'UKG_BTC', 'UKG_ETH', 'UKG_USDT', 'UTK_BTC', 'UTK_ETH', 'UTK_USDT', 'VEE_BTC', 'VEE_ETH', 'VEE_USDT', 'VIB_BTC', 'VIB_ETH', 'VIB_USDT', 'VIU_BTC', 'VIU_ETH', 'VIU_USDT', 'WIN_BTC', 'WIN_ETH', 'WIN_USDT', 'WRC_BTC', 'WRC_ETH', 'WRC_USDT', 'WTC_BTC', 'WTC_ETH', 'WTC_USDT', 'XRP_BTC', 'XRP_ETH', 'XRP_USDT', 'XUC_BTC', 'XUC_ETH', 'XUC_USDT', 'YEE_BTC', 'YEE_ETH', 'YEE_USDT', 'YOU_BTC', 'YOU_ETH', 'YOU_USDT', 'ZCO_BTC', 'ZCO_ETH', 'ZEC_BTC', 'ZEC_ETH', 'ZEC_USDT', 'ZEN_BTC', 'ZEN_ETH', 'ZEN_USDT', 'ZIL_BTC', 'ZIL_ETH', 'ZIL_USDT', 'ZIP_BTC', 'ZIP_ETH', 'ZIP_USDT', 'ZRX_BTC', 'ZRX_ETH', 'ZRX_USDT']
#symbols =['1ST_USDT']
def produce_json_data_list(symbols):
    json_dicts=[]
    json_data_list = []
    for symbol in symbols:
        channel = 'ok_sub_spot_%s_depth_20' % (symbol.lower())
        json_dict = {}
        json_dict['event'] = 'addChannel'
        json_dict['channel'] = channel
        json_dicts.append(json_dict)
    for j in json_dicts:
        json_data_list.append(j)
    return json.dumps(json_data_list)

def on_message(ws, message):
    if len(message)<200:
        return 
    exchange = 'okex'
    message=ast.literal_eval(message)
    message = message[0]
    symbol_list = message['channel'].split('_')
    symbol =  symbol_list[3].upper() + symbol_list[4].upper() 
    data = message['data']
    asks_raw = data['asks']
    asks_after = []
    for ask in asks_raw:
        item = list()
        item.append(float(ask[0]))  ##价格
        item.append(float(ask[1]))  ##数量
        asks_after.append(item)

    bids_raw = data['bids']
    bids_after = list()
    for bid in bids_raw:
        item = list()
        item.append(float(bid[0]))    ##价格
        item.append(float(bid[1]))    ##数量
        bids_after.append(item)
    value = {'bids':bids_after,'asks':asks_after,'source':exchange}
    value_final = json.dumps(value)
    print('putting okex......')
    r.hset(symbol,exchange,value_final)


async def sendInitMesg(ws):
    msg = produce_json_data_list(symbols)
    await ws.send(msg)

async def handleMessage(ws,URL):

    while True:
        try:
            greeting = await ws.recv()
            on_message(None,greeting)
        
        except websockets.exceptions.ConnectionClosed:
            print('fuck...................')
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

    URL = 'wss://real.okex.com:10441/websocket'
    loop = asyncio.get_event_loop()
    
    loop.run_until_complete(main(URL))

    


 
