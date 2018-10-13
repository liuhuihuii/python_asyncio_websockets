#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis
import asyncio 
import websockets
import json
import random

pool = redis.ConnectionPool(host='54.168.51.34', port=6379, decode_responses=True)  ##decode_response=True，写入的键值对中的value为str类型,无则为字节类型

r = redis.Redis(connection_pool=pool)

symbols = ['ABT_ETH', 'ABT_USDT', 'AE_BTC', 'AE_ETH', 'AE_USDT', 'ARN_ETH', 'ATMI_ETH', 'BAT_BTC', 'BAT_ETH', 'BAT_USDT', 'BCD_BTC', 'BCD_USDT', 'BCH_BTC', 'BCH_USDT', 'BFT_ETH', 'BFT_USDT', 'BIFI_BTC', 'BIFI_USDT', 'BLZ_ETH', 'BLZ_USDT', 'BNB_USDT', 'BNT_ETH', 'BNTY_ETH', 'BNTY_USDT', 'BOE_ETH', 'BOE_USDT', 'BOT_ETH', 'BOT_USDT', 'BTC_USDT', 'BTM_BTC', 'BTM_ETH', 'BTM_USDT', 'BTO_ETH', 'BTO_USDT', 'BTS_BTC', 'BTS_USDT', 'CDT_ETH', 'CDT_USDT', 'COFI_ETH', 'COFI_USDT', 'CS_ETH', 'CS_USDT', 'CVC_ETH', 'CVC_USDT', 'DADI_ETH', 'DADI_USDT', 'DASH_BTC', 'DASH_USDT', 'DATA_ETH', 'DATA_USDT', 'DBC_BTC', 'DBC_ETH', 'DBC_USDT', 'DDD_BTC', 'DDD_ETH', 'DDD_USDT', 'DGD_ETH', 'DGD_USDT', 'DNT_ETH', 'DOCK_ETH', 'DOCK_USDT', 'DOGE_BTC', 'DOGE_USDT', 'DPY_ETH', 'DPY_USDT', 'DRGN_ETH', 'DRGN_USDT', 'ELEC_ETH', 'ELEC_USDT', 'ELF_ETH', 'ELF_USDT', 'EOS_BTC', 'EOS_ETH', 'EOS_USDT', 'EOSDAC_ETH', 'EOSDAC_USDT', 'ETC_BTC', 'ETC_ETH', 'ETC_USDT', 'ETH_BTC', 'ETH_USDT', 'FTI_ETH', 'FTI_USDT', 'FUEL_ETH', 'FUEL_USDT', 'FUN_ETH', 'FUN_USDT', 'GARD_ETH', 'GARD_USDT', 'GAS_BTC', 'GAS_USDT', 'GEM_ETH', 'GEM_USDT', 'GNT_ETH', 'GNT_USDT', 'GNX_ETH', 'GNX_USDT', 'GSE_ETH', 'GSE_USDT', 'GTC_BTC', 'GTC_ETH', 'GTC_USDT', 'GXS_BTC', 'GXS_USDT', 'HAV_ETH', 'HAV_USDT', 'HT_USDT', 'ICX_ETH', 'ICX_USDT', 'IHT_ETH', 'IHT_USDT', 'INK_BTC', 'INK_ETH', 'INK_USDT', 'JNT_BTC', 'JNT_ETH', 'JNT_USDT', 'KICK_ETH', 'KICK_USDT', 'KNC_ETH', 'KNC_USDT', 'LBA_ETH', 'LBA_USDT', 'LEDU_BTC', 'LEDU_ETH', 'LEMO_ETH', 'LEMO_USDT', 'LEND_ETH', 'LEND_USDT', 'LINK_ETH', 'LINK_USDT', 'LRC_BTC', 'LRC_ETH', 'LRC_USDT', 'LRN_ETH', 'LRN_USDT', 'LTC_BTC', 'LTC_USDT', 'LUN_ETH', 'LUN_USDT', 'LYM_BTC', 'LYM_ETH', 'LYM_USDT', 'MAN_ETH', 'MAN_USDT', 'MANA_ETH', 'MANA_USDT', 'MCO_ETH', 'MCO_USDT', 'MDA_ETH', 'MDA_USDT', 'MDS_ETH', 'MDS_USDT', 'MDT_BTC', 'MDT_ETH', 'MDT_USDT', 'MED_ETH', 'MED_USDT', 'MEDX_ETH', 'MEDX_USDT', 'MET_ETH', 'MET_USDT', 'MKR_ETH', 'MKR_USDT', 'MTN_ETH', 'MTN_USDT', 'NAS_BTC', 'NAS_ETH', 'NAS_USDT', 'NEO_BTC', 'NEO_USDT', 'NKN_ETH', 'NKN_USDT', 'NPXS_ETH', 'OAX_ETH', 'OCN_BTC', 'OCN_ETH', 'OCN_USDT', 'OMG_BTC', 'OMG_ETH', 'OMG_USDT', 'ONT_ETH', 'ONT_USDT', 'OST_BTC', 'OST_ETH', 'OST_USDT', 'PAY_BTC', 'PAY_ETH', 'PAY_USDT', 'PLY_ETH', 'POWR_BTC', 'POWR_ETH', 'POWR_USDT', 'PST_ETH', 'PST_USDT', 'QASH_BTC', 'QASH_ETH', 'QASH_USDT', 'QBT_ETH', 'QBT_USDT', 'QKC_ETH', 'QKC_USDT', 'QLC_BTC', 'QLC_ETH', 'QLC_USDT', 'QSP_ETH', 'QSP_USDT', 'QTUM_BTC', 'QTUM_ETH', 'QTUM_USDT', 'RATING_ETH', 'RATING_USDT', 'RDN_ETH', 'RDN_USDT', 'RED_ETH', 'RED_USDT', 'RDN_ETH', 'RDN_USDT', 'REM_ETH', 'REM_USDT', 'REP_ETH', 'REQ_ETH', 'REQ_USDT', 'RFR_ETH', 'RFR_USDT', 'RLC_ETH', 'RLC_USDT', 'RUFF_BTC', 'RUFF_ETH', 'RUFF_USDT', 'SALT_ETH', 'SALT_USDT', 'SENC_ETH', 'SENC_USDT', 'SKM_ETH', 'SKM_USDT', 'SMT_ETH', 'SMT_USDT', 'SNET_ETH', 'SNET_USDT', 'SNT_BTC', 'SNT_ETH', 'SNT_USDT', 'SOP_ETH', 'SOP_USDT', 'SOUL_ETH', 'SOUL_USDT', 'STORJ_BTC', 'STORJ_ETH', 'STORJ_USDT', 'STX_ETH', 'STX_USDT', 'SWTH_ETH', 'SWTH_USDT', 'TCT_ETH', 'TCT_USDT', 'THETA_ETH', 'THETA_USDT', 'TIPS_ETH', 'TNC_BTC', 'TNC_ETH', 'TNC_USDT', 'TNT_ETH', 'TNT_USDT', 'TOMO_ETH', 'TOMO_USDT', 'TRX_ETH', 'TRX_USDT', 'TSL_USDT', 'VET_ETH', 'VET_USDT', 'WINGS_ETH', 'XLM_BTC', 'XLM_ETH', 'XLM_USDT', 'XMC_BTC', 'XMC_USDT', 'XMR_BTC', 'XMR_USDT', 'XRP_BTC', 'XRP_USDT', 'XTZ_BTC', 'XTZ_ETH', 'XTZ_USDT', 'XVG_BTC', 'XVG_USDT', 'ZEC_BTC', 'ZEC_USDT', 'ZIL_ETH', 'ZIL_USDT', 'ZRX_BTC', 'ZRX_ETH', 'ZRX_USDT', 'ZSC_ETH', 'ZSC_USDT']
#symbols = ['BTC_USDT']
 
##一组订阅事件list## 
def wrapEventList(symbols):
    params_list = []
    for symbol in symbols:
        param = []
        param.append(symbol)
        param.append(20)          ##limit
        param.append("0.00000001")         ##interval
        params_list.append(param)
    return params_list

def wrapStr2FloatItemList(data_str_list):
    data_float_list = list()
    for item in data_str_list:
        item_1 = []
        item_1.append(float(item[0]))
        item_1.append(float(item[1]))
        data_float_list.append(item_1)
    return data_float_list

##return combined update list
def produceSafisfiedList(old_list,new_list,bid_or_ask):
    for item in new_list:
        price = item[0]
        amount = item[1]
        if amount == 0:
            for i in range(len(old_list)):
                if price == old_list[i][0]:
                    old_list.pop(i)
                    break
                
        if amount != 0:
            flag = True
            for i in range(len(old_list)):
                if price == old_list[i][0]:
                    old_list[i][1] = amount
                    flag = False
                    break
                if price != old_list[i][0]:
                    continue
            ##追加价格价格不等,数量不为0
            if flag:
                old_list.append(item)
            
    ###排序，ask升序,bid降序    
    old_list.sort(key=lambda x:(x[0]),reverse = bid_or_ask)
    return old_list

##ws.recv()消息处理####
def on_message(ws, message):
    msg = json.loads(message)
    if msg['id'] is not None:
        return
    exchange = 'gateIO'
    symbol = msg['params'][-1].replace('_','')
    flag = msg['params'][0]     ##Boolean类型，增量更新还是全量更新
    if flag:
        ###直接设置即可
        dict_value = msg['params'][1]
        asks_after = wrapStr2FloatItemList(dict_value['asks'])
        bids_after = wrapStr2FloatItemList(dict_value['bids'])

        value_final ={'bids':bids_after,'asks':asks_after,'source':exchange}
        r.hset(symbol,exchange,json.dumps(value_final))
        print('putting gateIO.........')
    else:                       ##处理增量更新
        ##获取上一次redis数据
        old_dict_from_redis = json.loads(r.hget(symbol,exchange))
        ###update msg
        value_dict = msg['params'][1]                    
        if 'asks' in value_dict:
            asks_float_list = wrapStr2FloatItemList(value_dict['asks'])
            asks_float_list_update = produceSafisfiedList(old_dict_from_redis['asks'],asks_float_list,False)
            bids_float_list_old = old_dict_from_redis['bids']

            value_final ={'bids':bids_float_list_old,'asks':asks_float_list_update,'source':exchange}
            r.hset(symbol,exchange,json.dumps(value_final))
            print('putting gateIO.........')
        if 'bids' in value_dict:
            bids_float_list = wrapStr2FloatItemList(value_dict['bids'])
            bids_float_list_update = produceSafisfiedList(old_dict_from_redis['bids'],bids_float_list,True)
            asks_float_list_old = old_dict_from_redis['asks']

            value_final ={'bids':bids_float_list_update,'asks':asks_float_list_old,'source':exchange}
            r.hset(symbol,exchange,json.dumps(value_final))
            print('putting gateIO.........')

        if  'asks' in value_dict and 'bids' in value_dict:
            asks_after = wrapStr2FloatItemList(value_dict['asks'])
            bids_after = wrapStr2FloatItemList(value_dict['bids']) 

            asks_float_list_update = produceSafisfiedList(old_dict_from_redis['asks'],asks_after,False)
            bids_float_list_update = produceSafisfiedList(old_dict_from_redis['bids'],bids_after,True)
            value_final ={'bids':bids_float_list_update,'asks':asks_float_list_update,'source':exchange}
            r.hset(symbol,exchange,json.dumps(value_final))
            print('putting gateIO.........')
            

async def sendInitMesg(ws):
    params = wrapEventList(symbols)
    event_dict={'id':random.randint(0,99999),"method":"depth.subscribe", "params":params}
    await ws.send(json.dumps(event_dict)) 

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

    URL = 'wss://ws.gate.io/v3/'
    loop = asyncio.get_event_loop()
    
    loop.run_until_complete(main(URL))

    


 
