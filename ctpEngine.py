# encoding: UTF-8
from string import lowercase as _chars
from carbon import Carbon,Couple
from settings_account import *
from settings_mongo import *
from settings_ctp import *
from random import randint
from string import lowercase as _chars
from string import uppercase as _CHARS
from log import *
from ctp_data_type import defineDict
import urllib2,random
from demo_api import DEMO_ID
from math import log as mathlog
from mmtt import *
from mmgg import *
from make_double_tick import make_tick
from cmath import sqrt as math_sqrt

def sqrt(v):
    return math_sqrt(v).real

def ema(v,pv,ma):
    _k = 2.0/(1+ma)
    return v*_k+pv*(1-_k)

def std(_ma,_list):
    return sqrt(sum([(_ma-x)**2 for x in _list])/len(_list))

def make_plus(accountid):
    return ''.join([_chars[int(x)] for x in accountid])

def expiredate_shift(date,_days = 30):
    d = dt.datetime.strptime(date,'%Y%m%d')
    nd = d - dt.timedelta(days=_days)
    return nd.strftime('%Y%m%d')

_TODAYPOSITIONDATE_ = defineDict["THOST_FTDC_PSD_Today"]#'1'
_YDPOSITIONDATE_    = defineDict["THOST_FTDC_PSD_History"]#'2'

_LONGDIRECTION_     = defineDict["THOST_FTDC_PD_Long"]#'2'
_SHORTDIRECTION_    = defineDict["THOST_FTDC_PD_Short"]#'3'


_dir_ = {'0':u'买','1':u'卖'}
_dir_default = u'notFound Direction'
_off_ = {'0':u'开','1':u'平','3':u'平今','4':u'平昨'}
_off_default = u'notFound OffsetFlag'
_loc_ = {0:u'异地',1:u'本地'}
_loc_default = u'notFound locationInfo'

_Exchange_ = ['SHFE', 'CFFEX']

from ctpApi import *
from eventEngine import EventEngine
from threading import Lock
import traceback

def get_master():
    _day = dt.datetime.now()
    _add = dt.timedelta(days=1)
    while _day.isoweekday()==5:
        _day = _day+_add
    c = conn[DB_NAME]['symbols']
    c.delete_many({'ProductID':'m_o'})
    c.delete_many({'ProductID':'SRP'})
    c.delete_many({'ProductID':'SRC'})
    _all = c.find({'ExpireDate':{'$gt':_day.strftime('%Y%m%d')}},{'_id':0},sort=[('_vol_',desc),('InstrumentID',asc)])
    pd_dict = {}
    pd_first = {}
    pd_secend = {}
    for one in list(_all):
        _pd = one['ProductID']
        pd_dict[_pd] = 1
        if one['_master_'] == 2:
            pd_first[_pd] = {'symbol':one['InstrumentID'],'vol':one.get('_vol_',0),'date':one['ExpireDate']}
        elif one['_master_'] == 1:
            pd_secend[_pd] = {'symbol':one['InstrumentID'],'vol':one.get('_vol_',0),'date':one['ExpireDate']}

    for _pd in pd_dict.keys():
        _pd_list = list(conn[DB_NAME]['symbols'].find({'ProductID':_pd,'ExpireDate':{'$gt':_day.strftime('%Y%m%d')}},{'_id':0},\
                        sort=[('_vol_',desc),('InstrumentID',asc)]))
        if _pd in pd_first:
            _old = pd_first[_pd]
            _new_list = filter(lambda x:x['ExpireDate']>_old['date'] and x.get('_vol_',0)>_old['vol'],_pd_list)
            if _new_list:
                one = _new_list[0]
                add_log(u'主力合约迁移 %s => %s'%(_old['symbol'],one['InstrumentID']),3600)
                #==================================================================================================
                conn[DB_NAME]['changeMaster'].insert_one({'ProductID':_pd,'change':dt.datetime.now().strftime('%Y%m%d'),'symbol':one['InstrumentID']})
                conn[DB_NAME]['changeMaster'].delete_many({'ProductID':_pd,'change':{'$lt':str(int(_day.strftime('%Y%m%d'))-10000)}})
                pd_first[_pd] = {'symbol':one['InstrumentID'],'vol':one['_vol_'],'date':one['ExpireDate']}
        else:
            one = _pd_list[0]
            add_log(u'主力合约初始化 %s'%one['InstrumentID'],3600)
            pd_first[_pd] = {'symbol':one['InstrumentID'],'vol':one.get('_vol_',0),'date':one['ExpireDate']}
            conn[DB_NAME]['changeMaster'].insert_one({'ProductID':_pd,'change':dt.datetime.now().strftime('%Y%m%d'),'symbol':one['InstrumentID']})

        if _pd in pd_secend and pd_secend[_pd]['symbol']==pd_first[_pd]['symbol']:
            pd_secend.pop(_pd)
        if _pd in pd_first and _pd in pd_secend and pd_secend[_pd]['date']<pd_first[_pd]['date']:
            pd_secend.pop(_pd)

        if _pd in pd_secend:
            _old = pd_secend[_pd]
            _new_list = filter(lambda x:x['InstrumentID']!=pd_first[_pd]['symbol'] and x['ExpireDate']>_old['date'] and x.get('_vol_',0)>_old['vol'],_pd_list)
            if _new_list:
                one = _new_list[0]
                add_log(u'次主力合约迁移 %s => %s'%(_old['symbol'],one['InstrumentID']),3600)
                pd_secend[_pd] = {'symbol':one['InstrumentID'],'vol':one['_vol_'],'date':one['ExpireDate']}
        else:
            n = 0
            while not pd_secend.get(_pd,{}) and n<len(_pd_list):
                if _pd_list[n]['InstrumentID']!=pd_first[_pd]['symbol'] and _pd_list[n]['ExpireDate']>pd_first[_pd]['date']:
                    one = _pd_list[n]
                    add_log(u'次主力合约初始化 %s'%one['InstrumentID'],3600)
                    pd_secend[_pd] = {'symbol':one['InstrumentID'],'vol':one.get('_vol_',0),'date':one['ExpireDate']}
                n += 1
    logger.error('=================1st %d'%len(pd_first))
    for _pd in pd_dict.keys():
        c.update({'ProductID':_pd},{'$set':{'_master_':0}},multi=True)
        if _pd in pd_first:
            c.update({'InstrumentID':pd_first[_pd]['symbol']},{'$set':{'_master_':2}})
        if _pd in pd_secend:
            c.update({'InstrumentID':pd_secend[_pd]['symbol']},{'$set':{'_master_':1}})
    logger.error('=================2nd %d'%len(pd_secend))
    #===============================================================================
    _all = c.find({'_master_':{'$gt':-1}},{'ExpireDate':1,'InstrumentID':1,'ShortMarginRatio':1,'LongMarginRatio':1,'VolumeMultiple':1,'ProductID':1,'ExchangeID':1,'_vol_':1,'_lastday':1,'_master_':1,'_id':0},sort=[('_vol_',desc),('InstrumentID',asc)])
    out = []
    for one in list(_all):
        if one['ProductID'] not in ['SRP','SRC','m_o','cu_o','au_o','ag_o']:
            out.append(one)
    return json.dumps(out)

conn[DB_NAME]['SignalHistory'].create_index([('date',desc),('client_version',desc),('time',desc),('traded',desc),('op',desc),('InstrumentID',desc),('account',desc),('OrderRef',desc)],name='baseindex20160909',background=True)
conn[DB_NAME]['LogicHistory'].create_index([('date',desc),('tradeid',desc),('logic',desc),('InstrumentID',desc)],name='logicindex20160909',background=True)
conn[DB_NAME]['doubletable'].create_index([('InstrumentID',desc),('master',desc),('isTrade',desc),('cnt',desc),('b',desc),('a',desc)],background=True)
conn[DB_NAME]['doubletable'].create_index([('gridlevel',asc),('bigtimer',asc),('big',asc),('atop',asc),('btop',asc),('close',desc),('move',desc),('master',desc),('isTrade',desc),('account',desc)],background=True)
conn[DB_NAME]['tick'].create_index([('num',desc),('symbol',desc),('time',asc),('day',asc),('master',asc)],background=True)
conn[DB_NAME]['doubleid'].create_index([('symbol',desc),('product',desc),('a',desc),('b',desc),('voldate',desc)],background=True)
conn[DB_NAME]['doubleid'].create_index([('historyk',desc)],background=True)
conn[DB_NAME]['changeMaster'].create_index([('ProductID',desc),('change',desc)],background=True)
conn[DB_NAME]['doublek'].create_index([('symbol',desc),('group',desc),('n',desc),('time',desc),('do',desc)],background=True)
conn[DB_NAME]['state'].create_index([('version',desc),('symbol',desc)],background=True)
conn[DB_NAME]['realeq'].create_index([('account',desc),('time',desc),('minute',desc)],background=True)
conn[DB_NAME]['platten'].create_index([('symbol',desc),('account',desc),('tradingday',desc),('time',desc)],background=True)


print 'tick : ',conn[DB_NAME]['tick'].count()

########################################################################
class MainEngine:

    #----------------------------------------------------------------------
    def __init__(self,fake=False):
        self.init_time = time.time()
        self.percent_top_limit = {}
        self.check_platten = 0
        self.check_platten_time = 0
        self.account_percent = {}
        self.platten_init = -1
        self.cleaning = False
        self.symbolInfoCache = {}
        self.init()
        self.fake = fake
        self.init_tick_accounts()
        self.init_trade_accounts()
    def init(self):
        self.makeTicking = 0
        self.db = conn[DB_NAME]
        self.db['heart'].delete_many({})
        self.master_list = json.loads(get_master())
        add_log(u'有效合约 %d 个'%len(self.master_list),3600)
        self.master = {}
        self.masterDouble = {}
        for _inst in list(self.db['doubleMaster'].find()):
            self.masterDouble[_inst['symbol']] = 1
        self.other_Double = {}
        self.masterPd = {}
        self.instInfo = {}
        self.vol = {}
        self.tickClient = {}
        self.tradeClient = {}
        self.tickLog = {}
        self.posCache = {}
        self.rebootTimer = time.time()+60
        self.rebootCount = {}
        self.rebootTarget = {}
        self.ended = False
        self.init_date = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.nextCheck = 0
        self.registVol = False
        self.instCache = {}
        self.tickCache = {}
        self.tickHistoryCal = {}
        self.doubleCache = {}
        self.doublePriceCache = {}
        self.productPower = {}
        self.doublePower = {}
        self.doubleMax = {}
        self.doubleMin = {}
        self.tickTimer = time.time()-10
        self.fork_counter = 0
        self.fork_limit = 30
        self.fork_job_list = range(self.fork_limit)
        self.tradingday = '0'
        self.account_history = {}
        # init
        self.ee = EventEngine({},CheckTimer=False)         # 创建事件驱动引擎
        self.ee.start()                 # 启动事件驱动引擎
        try:
            self.bridge = Bridge(self.ee,1)
        finally:
            pass
        self.ee.register(MQTT_EVENT_MARGIN,   self.mqtt_margin)
        self.ee.register(EVENT_TIMER,       self.timerSaveSymbolInfo)     #
        self.ee.register(EVENT_TIMER,       self.get_heart)             #
        self.ee.register(EVENT_TIMER,       self.check4reboot)     #
        self.ee.register(EVENT_TIMER,       self.check_account_platten)     #
        self.ee.register(EVENT_TIMER,       self.get_plus)             #
        self.ee.register(EVENT_TIMER,       self.demo_account)             #
        self.ee.register(EVENT_TIMER,       self.fork_job)             #
        self.ee.register(EVENT_TIMER,       self.reset_symbol)             #
        self.ee.register(EVENT_ERROR,       self.get_error)     #
        self.ee.register(EVENT_LOG,         self.get_log)       #
    def mqtt_margin(self,event):
        _data = event.dict_['data']
    def reset_symbol(self,e):
        if self.db['reset'].count()==0:
            return 0
        _all = list(self.db['reset'].find())
        for one in _all:
            symbol = one['symbol']
            if symbol in self.doubleCache:
                self.doubleCache.pop(symbol)
            self.db['state'].delete_many({'symbol':symbol})
            self.db['doubletable'].delete_many({'InstrumentID':symbol})
            self.db['doublek'].delete_many({'symbol':symbol})
            if '_' in symbol:
                _inst1,_inst2 = symbol.split('_')
                conn[_inst1][_inst2+'pf'].delete_many({})
                conn[_inst1][_inst2].delete_many({})
                conn[_inst1]['double'].delete_many({'symbol':symbol})
                conn[_inst2]['double'].delete_many({'symbol':symbol})
            self.db['reset'].delete_many({'symbol':symbol})
            logger.error('reset ok %s'%symbol)

    def fork_job(self,e):

        def job_fork_demo(id):
            n = randint(1,20)
            print '[%d] fork test begin %d %s'%(id,n,datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            time.sleep(n)
            print '[%d] fork test end %d %s'%(id,n,datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            self.fork_job_list.append(id)

        def job_fork(id):
            try:
                _do = False
                if self.doublePriceCache and len(self.doublePriceCache)>id:
                    _time = t1 = time.time()
                    _all = [ (v,k) for k,v in self.doublePriceCache.items()]
                    _all.sort()
                    _job = _all[-1*(id+1)]
                    _key = _job[1]
                    _cha =      _job[0][0]
                    _new =      _job[0][1]
                    _old =      _job[0][2]
                    _master =   _job[0][3]
                    if _cha>0:
                        self.doublePriceCache[_key] = (0,_new,_new,_master)
                        _do = True
                        if _key in self.masterDouble:
                            o = self.get_double_object(_key, self.instInfo)
                            o.new_price(time.time(),_new,self.tradingday)
                            o.get_trade(self.tickCache)
                            _timer = time.time()-_time
                            th_fork(logger.error,('[%d] <a href="/image/%s/0000/?_=%.0f" target="_blank">%s</a> %.5f => %.5f <%.5f> len:%d|%d time:%.3f'%(id,_key,time.time(),_key,1.0*_old,1.0*_new,1.0*_cha,len(self.masterDouble),len(self.doublePriceCache),_timer),))
                        else:
                            _t = int(time.time())
                            _inst_list = _key.split('_')
                            _num = -1
                            if _key in self.other_Double:
                                _num = self.other_Double[_key][0]
                                _voldate = self.other_Double[_key][1]
                            else:
                                _haved = self.db['doubleid'].find_one({'symbol':_key}) or {}
                                if _haved:
                                    _num = sum([ord(x) for x in str(_haved['_id'])])%self.fork_limit
                                    _voldate = _haved.get('voldate',1)
                                    self.db['doubleid'].update({'_id':_haved['_id']},{'$set':{'num':_num,'a':_inst_list[0],'b':_inst_list[1]}})
                                    self.other_Double[_key] = (_num,_voldate)
                            if _voldate>0:
                                _info = {_inst_list[0]:self.instInfo[_inst_list[0]],_inst_list[1]:self.instInfo[_inst_list[1]]}
                                _tick = {_inst_list[0]:self.tickCache[_inst_list[0]],_inst_list[1]:self.tickCache[_inst_list[1]]}
                                self.db['tick'].insert_one({'from':'real','num':_num,'time':_t,'master':_voldate,'day':self.tradingday,'symbol':_key,'price':_new,'info':_info,'tick':_tick})
                if not _do and time.time()-self.tickTimer>60:
                    if self.db['tick'].count()==0 and id==0:
                        #   back test
#                        self.makeTicking = 1
#                        make_tick()
#                        self.makeTicking = 0
                        #   back test   end
                        self.alpha2digit = {}
                        if len(self.doubleCache)==0 and self.instInfo:
                            for _inst in list(self.db['doubleMaster'].find()):
                                _b = time.time()
                                self.get_double_object(_inst['symbol'],self.instInfo)
                                logger.error(u'预加载 %s %.5f'%(_inst['symbol'],time.time()-_b))
                        time.sleep(id+1)
                    elif self.makeTicking == 0:
                        _tick = self.db['tick'].find_one({'num':id},sort=[('day',desc),('master',desc),('symbol',desc)])
                        if _tick:
                            _inst = _tick['symbol']
                            _master = _tick['master']
                            _day = _tick['day']
                            o = self.get_double_object(_inst,_tick['info'])
                            if o.noTrade:   # tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt
                                rs = self.db['tick'].delete_many({'symbol':_inst})
                                _a,_b = _inst.split('_')
                                conn[_a].drop_collection(_b)
                                conn[_a].drop_collection(_b+'pf')
                                self.doubleCache.pop(_inst)
                                self.db['state'].delete_many({'symbol':_inst})
                                rsk = self.db['doublek'].delete_many({'symbol':_inst})
                                logger.error(u'%s 移除缓存，清理K线[%d]'%(_inst,rsk.raw_result['n']))
                                logger.error(u'<font style="color:red">###[%d] 清理跨期合约数据 %s (%d|%d) %s[%d]</font>'%(id,_inst,rs.deleted_count,self.db['tick'].count(),_day,_master))
                            else:
                                _LEN_ = 200
                                _all = list(self.db['tick'].find({'symbol':_inst},sort=[('time',asc)],limit=_LEN_))
                                nn = 0
                                _last = 0.0
                                if _inst in self.masterDouble:
                                    self.masterDouble.pop(_inst)
                                    logger.error(u'移除主力合约缓存 %s'%_inst)
                                for _tick in _all:
                                    nn += 1
                                    o.new_price(_tick['time'],_tick['price'],_tick['day'])
                                    _last = _tick['price']
                                    self.db['tick'].delete_many({'_id':_tick['_id']})
                                if (len(_all)<_LEN_ or self.db['tick'].find({'symbol':_inst}).count()==0) and _inst in self.doubleCache and _inst not in self.masterDouble:
                                    self.doubleCache.pop(_inst)
                                    logger.error(u'移除缓存 %s'%_inst)
                                    #=======================
                                    if _tick.get('from','real') == 'make':
                                        logger.error('wait more tick 60s')
                                        time.sleep(60)
                                    #=======================
                                add_log(u'<font style="color:blue">[%d] <a href="/image/%s/0000/" target="_blank">%s</a> 分笔完成(%d) %s[%d]</font>'%(id,_inst,_inst,self.db['tick'].count(),_day,_master),100)
            except:
                logger.error(str(traceback.format_exc()))
            finally:
                self.fork_job_list.append(id)

        while self.fork_job_list:
            self.fork_id = self.fork_job_list.pop(0)
            try:
                th_fork(job_fork,(self.fork_id,))
            except:
                logger.error('th_fork error @ctpengine.fork_job')
                time.sleep(10)

    def check4double(self,event):
        _time = time.time()
        if not self.doublePriceCache:
            return 0
        _all = self.doublePriceCache.items()
        sort_all = [(v,k) for k,v in _all]
        sort_all.sort()
        _last = sort_all[-1]
        _key = _last[1]
        _cha = _last[0][0]
        _new = _last[0][1]
        _old = _last[0][-1]
        if _cha>0:
            if 1>0:
                o = self.get_double_object(_key, self.instInfo)
                o.new_price(time.time(),_new,self.tradingday)
                o.get_trade(self.tickCache)
            _timer = time.time()-_time
            logger.error('<a href="/image/%s/0000/" target="_blank">%s</a>,old:%.5f new:%.5f cha:%.5f len:%d time:%.3f'%(_key,_key,1.0*_old,1.0*_new,1.0*_cha,len(self.doublePriceCache),_timer))
            self.doublePriceCache[_key] = (0,_new,_new)
    def check4power(self,event):
        _time = time.time()
        if not self.doublePower:
            return 0
        _all = self.doublePower.items()
        sort_all = [(v,k) for k,v in _all]
        sort_all.sort()
        _last = sort_all[0]
        _key = _last[1]
        _pwr = _last[0]
        _list = _key.split('_')
        _inst_1 = self.masterPd[_list[0]]['InstrumentID']
        _inst_2 = self.masterPd[_list[1]]['InstrumentID']
        self.db['double'].update({'key':_key},{'$set':{'key':_key,'power':_pwr,'one':_inst_1,'two':_inst_2,'date':dt.datetime.now().strftime('%Y%m%d %H%M%S')}},upsert=True)
        self.doublePower.pop(_key)
    def get_double_object(self,_inst,infoDict):
        if _inst in self.doubleCache:
            o = self.doubleCache[_inst]
            o.trade_account(self.tradeClient)
            return o
        else:
            o = Couple(_inst,infoDict,conn)
            o.load_state()
            if o.setMaster(self.instInfo)>0:
                self.masterDouble[_inst] = 1
                logger.error(u'<font style="color:grey">...已初始化对冲品种主力合约 (%d) ...</font>'%len(self.masterDouble))
            elif _inst in self.masterDouble:
                self.masterDouble.pop(_inst)
            o.trade_account(self.tradeClient)
            self.doubleCache[_inst] = o
            if len(self.doubleCache.keys())%10==0:
                logger.error(u'<font style="color:grey">已初始化对冲品种 (%d)</font>'%len(self.doubleCache.keys()))
            return o
    def tick2double(self,event):
        _data = event.dict_['data']
        _inst = _data['InstrumentID']
        self.tickTimer = time.time()
        self.tickCache[_inst] = _data
        _ask = _data['AskPrice1']
        _bid = _data['BidPrice1']
        if min(_ask,_bid)/max(_ask,_bid) < 0.9:
            self.db['doubletable'].update({'a': _inst}, {'$set': {'atop': 1}}, multi=True)
            self.db['doubletable'].update({'b': _inst}, {'$set': {'btop': 1}}, multi=True)
            add_log(u'<b>### %s 涨跌停 !!!</b>'%_inst,1000)
        for _tick in self.tickCache.values():
            _master = _data['isMaster']*_tick['isMaster']
            _pd_list = [_data['ProductID'] , _tick['ProductID']]
            _pd_list.sort()
            if _data['ProductID'] != _tick['ProductID'] and '_'.join(_pd_list) in DOUBLE_RUN_SET:
                pd_list = [(_data['ProductID'],_data['InstrumentID'],_data['LastPrice']),(_tick['ProductID'],_tick['InstrumentID'],_tick['LastPrice'])]
                pd_list.sort()
                pd_key = '_'.join([x[0] for x in pd_list])
                inst_key = '_'.join([x[1] for x in pd_list])
                if 1:
                    _k = mathlog(pd_list[0][-1])-mathlog(pd_list[1][-1])
                    _k_old = self.doublePriceCache.get(inst_key,(0,_k,_k,0))
                    _c = abs( _k - _k_old[2] )
                    if inst_key not in self.masterDouble and inst_key not in self.other_Double:
                        self.db['doubleid'].update({'symbol':inst_key},{'$set':{'time':time.time(),'historyk':time.time(),'a':pd_list[0][1],'b':pd_list[1][1],'symbol':inst_key,'product':pd_key}},upsert=True)
                    self.doublePriceCache[inst_key] = ( _c , _k , _k_old[2] ,_master)
    def get_heart(self,event):
        self.bridge.heart()
        if event.dict_['_account_'] == '*':
            self.db['heart'].update({'id':'heart','account':'*','type':'*'},{'$set':{'time':time.time(),'account':'*','type':'*'}},upsert=True)
            return
        else:
            _account = event.dict_['_account_']
            _type = event.dict_['_type_']
            self.db['heart'].update({'id':'heart','account':_account,'type':_type},{'$set':{'time':time.time(),'account':_account,'type':_type}},upsert=True)
    def check4reboot(self,event):
        def reconnect():
            if dt.datetime.now().isoweekday() in [6,7]:
                add_log(u'周末跳过重启',300)
                return 0
#            self.bridge = Bridge(self.ee,1)
            try:
                self.bridge.reboot()
            finally:
                pass
            self.tickHistoryCal = {}
            self.master_list = json.loads(get_master())
            add_log(u'有效合约 %d 个'%len(self.master_list),3600)
            self.ended = False
            self.instInfo = {}
            self.db['heart'].delete_many({})
            self.master = {}
            self.masterDouble = {}
            for _inst in list(self.db['doubleMaster'].find()):
                self.masterDouble[_inst['symbol']] = 1
            self.other_Double = {}
            self.masterPd = {}
            self.vol = {}
            self.tickLog = {}
            self.posCache = {}
            self.rebootCount = {}
            self.init_date = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.instCache = {}
            self.tickCache = {}
            self.tickTimer = time.time()-10
            self.doubleCache = {}
            self.doublePriceCache = {}
            self.tradingday = '20170101'
            self.account_history = {}
            if dt.datetime.now().isoweekday()>5:
                return 0
            for one in self.tickClient.values():
                one['md'].connect_server()
            for one in self.tradeClient.values():
                if 'td' in one:
                    one['td'].connect_server()

        if time.time()<self.rebootTimer:
            return 0
        _time = event.dict_['_time_']
        if _time.hour==8 and _time.minute==49:
            self.rebootCount = {}
        if _time.hour==8 and _time.minute>=50 and time.time()>self.rebootTimer and len(self.rebootCount)!=len(self.rebootTarget):
            self.rebootTimer = time.time()+120
            reconnect()
        if _time.hour==12 and _time.minute==49:
            self.rebootCount = {}
        if _time.hour==12 and _time.minute>=50 and time.time()>self.rebootTimer and len(self.rebootCount)!=len(self.rebootTarget):
            self.rebootTimer = time.time()+120
            reconnect()
        if _time.hour==20 and _time.minute==49:
            self.rebootCount = {}
        if _time.hour==20 and _time.minute>=50 and time.time()>self.rebootTimer and len(self.rebootCount)!=len(self.rebootTarget):
            self.rebootTimer = time.time()+120
            reconnect()
        if not self.ended:
            if _time.hour in [16,17,18,19,3,4,5,6,7]:
                print('exit...')
                self.ended = True
            if _time.hour in [15,2] and _time.minute in [30]:
                print('exit 30===')
                self.ended = True
    def plus_table(self):
        _all = self.db['table'].find({})
        n = 0
        for o in _all:
            if o.get('sum',0)!=o.get('table',0) and o.get('account','')!=DEMO_ID:
                n += 1
                self.db['table'].update({'_id':o['_id']},{'$set':{'table':o.get('sum',0)}})
                self.db['plus'].update({'InstrumentID':o['InstrumentID']},{'$set':{'InstrumentID':o['InstrumentID'],'account':o['account'],'table':o.get('sum',0)}},upsert=True)
                logger.error('plus_table %s'%str(o))
        event = Event(type_=EVENT_LOG)
        log = u'<font color="green">配平完成 %d</font>'%n
        event.dict_['log'] = log
        self.ee.put(event)
    def get_plus(self,event):
        a = self.db['plus'].find({})
        for one in list(a):
            _inst = one['InstrumentID']
            self.db['plus'].delete_many({'InstrumentID':_inst})
            self.get_inst_object(_inst).plus_table(one['account'],one['table'])
    def init_tick_accounts(self):
        for acc in tick_accounts:
            _address = str(acc['mdfront'])
            _userid  = str(acc['account'])
            _password= str(acc['password'])
            _brokerid= str(acc['brokerid'])
            _pluspath= make_plus(_userid+_brokerid)
            acc['_type_'] = u'md'
            _ee = EventEngine(acc,CheckTimer=True)
            _ee.start()
            _ee.register(EVENT_MDLOGIN, self.ready_subscribe_single)
            _ee.register(EVENT_LOG,     self.get_log)
            _ee.register(EVENT_TICK,   self.get_heart)             #
            _ee.register(EVENT_ERROR,   self.get_error)
            if Save_Ask_Bid:
                _ee.register(EVENT_TICK,    self.save4askbid)
            _ee.register(EVENT_TICK,   self.tick2logic)
            _ee.register(EVENT_TICK,   self.tick2double)
            _ee.register(EVENT_TICK,   self.save4vol)
            acc['ee'] = _ee
            acc['md'] = ctpMdApi(_ee,_address,_userid,_password,_brokerid,plus_path=_pluspath)
            acc['password'] = '*'
            self.tickClient[_userid] = acc
            self.rebootTarget['md%s'%_userid] = 1
        logger.error('init_tick_accounts')
    def init_trade_accounts(self):
        for acc in trade_accounts:
            _address = str(acc['tdfront'])
            _userid  = str(acc['account'])
            _password= str(acc['password'])
            _brokerid= str(acc['brokerid'])
            _pluspath= make_plus(_userid+_brokerid)
            acc['_type_'] = u'td'
            _ee = EventEngine(acc,CheckTimer=False)
            _ee.start()
            _ee.register(EVENT_TDLOGIN, self.ready_trade_single)
            _ee.register(EVENT_LOG,     self.get_log)
            _ee.register(EVENT_ERROR,   self.get_error)
            _ee.register(EVENT_TIMER,   self.query_circle_in)
            _ee.register(EVENT_POSITION,   self.get_heart)             #
            _ee.register(EVENT_ACCOUNT,   self.get_heart)             #
            _ee.register(EVENT_POSITION,  self.onPosition_in)
            _ee.register(EVENT_ACCOUNT,   self.onAccount_in)
            _ee.register(EVENT_ORDER,   self.onOrder_in)
            _ee.register(EVENT_TRADE,   self.onTrade_in)
            _ee.register(EVENT_INSTRUMENT,  self.save_instrument_to_db)
            acc['ee'] = _ee
            acc['lastGet'] = 'Account'
            acc['td'] = ctpTdApi(_ee,_address,_userid,_password,_brokerid,plus_path=_pluspath)
            acc['password'] = '*'
            _status = self.db['account'].find_one({'account':_userid}) or {}
            acc['Balance'] = _status.get('Balance',1.0)
            self.tradeClient[_userid] = acc
            self.rebootTarget['td%s'%_userid] = 1
        _demo = self.db['account'].find_one({'account':DEMO_ID}) or {}
        self.tradeClient[DEMO_ID] = {'Balance':_demo.get('Balance',1.0),'account':DEMO_ID,'name':DEMO_Name}
        logger.error('init_trade_accounts')
        self.clear_db()
    def clear_db(self):
        for one in list(self.db['account'].find()):
            if one.get('account','') not in self.tradeClient and one.get('account','')!=DEMO_ID:
                self.db['account'].delete_many({'account':one.get('account','')})
#        for one in list(self.db['table'].find()):
#            if one.get('account','') not in self.tradeClient and one.get('account','')!=DEMO_ID:
#                self.db['table'].delete_many({'account':one.get('account','')})
        logger.error('clear_db')
    def get_inst_object(self,_inst):
        if _inst in self.instCache:
            o = self.instCache[_inst]
            o.setBridge(self.bridge)
            o.trade_account(self.tradeClient)
            if _inst in self.master:
                o.setMaster(self.master[_inst])
            else:
                o.setMaster({})
            return o
        else:
            _h = self.db['symbols'].find_one({'InstrumentID':_inst})
            if _h:
                o = Carbon(_inst,_h,conn,trade=isTrade)
                o.setBridge(self.bridge)
                o.setMaster(self.master.get(_inst,{}))
                o.load_state()
                o.trade_account(self.tradeClient)
                self.instCache[_inst] = o
                if len(self.instCache.keys())%10==0:
                    logger.error('inst object to %d'%len(self.instCache.keys()))
                return o
            elif _inst:
                _d = {}
                _d['log'] = u'<font color="red">未发现合约信息 %s</font>'%_inst
                _d['_time_'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                _d['time'] = time.time()
                self.db['error'].replace_one(_d,_d,upsert=True)
    def onPosition_in(self,event):
        _account = event.dict_['_account_']
        if _account not in self.posCache:
            self.posCache[_account] = {}
        _data = event.dict_['data']
        _symbol = _data['InstrumentID']
        if '&' not in _symbol:
            if _symbol not in self.posCache[_account]:
                self.posCache[_account][_symbol] = {'todaylong':0,'todayshort':0,'ydshort':0,'ydlong':0}
            _TODAYPOSITIONDATE_ = defineDict["THOST_FTDC_PSD_Today"]#'1'
            _YDPOSITIONDATE_    = defineDict["THOST_FTDC_PSD_History"]#'2'
            _LONGDIRECTION_     = defineDict["THOST_FTDC_PD_Long"]#'2'
            _SHORTDIRECTION_    = defineDict["THOST_FTDC_PD_Short"]#'3'
            if _data['PosiDirection'] == _LONGDIRECTION_ and _data['PositionDate'] == _TODAYPOSITIONDATE_:
                self.posCache[_account][_symbol]['todaylong'] = _data['Position']
            if _data['PosiDirection'] == _LONGDIRECTION_ and _data['PositionDate'] == _YDPOSITIONDATE_:
                self.posCache[_account][_symbol]['ydlong'] = _data['Position']
            if _data['PosiDirection'] == _SHORTDIRECTION_ and _data['PositionDate'] == _TODAYPOSITIONDATE_:
                self.posCache[_account][_symbol]['todayshort'] = _data['Position']
            if _data['PosiDirection'] == _SHORTDIRECTION_ and _data['PositionDate'] == _YDPOSITIONDATE_:
                self.posCache[_account][_symbol]['ydshort'] = _data['Position']
        if _data['last']:
            _check_lock = False
            _percent = self.account_percent.get(_account,.0)
            for k,v in self.posCache[_account].items():
                o = self.get_inst_object(k)
                if o:
                    if not _check_lock and _percent > TRADE_LOCK_TODAY + TRADE_LOCK_RANGE:
                        o.in_position(v,_account,clear_today_lock = True,account_percent = _percent)
                        _check_lock = True
                    elif not _check_lock and _percent > TRADE_LOCK_TODAY:
                        o.in_position(v, _account, clear_yd_lock = True,account_percent = _percent)
                        _check_lock = True
                    else:
                        o.in_position(v, _account,account_percent = _percent)

    def onOrder_in(self,event):
        _account = event.dict_['_account_']
        _data = event.dict_['data']
        _symbol = _data['InstrumentID']
        o = self.get_inst_object(_symbol)
        o.in_order(_data,_account)
    def onTrade_in(self,event):
        _account = event.dict_['_account_']
        _data = event.dict_['data']
        _symbol = _data['InstrumentID']
        o = self.get_inst_object(_symbol)
        o.in_trade(_data,_account)
    def demo_account(self,event):
        if self.tradingday=='':return

        def inTrade():
            _now = dt.datetime.now().strftime('%H%M')
            _int = int(_now)
            if 900 <= _int < 1015:
                return True
            elif 1030 <= _int < 1130:
                return True
            elif 1330 <= _int < 1500:
                return True
            elif 2100 <= _int < 2359:
                return True
            else:
                return False

        def grid_profit(d,p):
            s = 0.0
            for k,v in d.items():
                s += (p-v['open'])*v['trend']
            return s/8.0
        if not runDemo:return
        if time.time()-self.tickTimer>5:
            return
        _data = Demo_Update()
        _account = _data['account']
        self.tradeClient[_account] = _data
        _data['TradingDay'] = self.tradingday
        _data['account'] = DEMO_ID
        _data['_time'] = time.time()
        _data['_marginshift_'] = {'a':0.0,'b':1.0,'c':0.0}
        _data['_riskshift_'] = {'x':0.0,'y':1.0,'z':0.0}
        _data['_begin_'] = DEMO_Money
        _data['accupdate'] = dt.datetime.now().strftime('%H%M%S')
        rs = self.db['account'].update({'account':DEMO_ID},{'$set':_data},upsert=True)
        eq = _data['Balance']
        if inTrade():
            conn[DB_NAME]['realeq'].update({'account':DEMO_ID,'minute':int(time.time()/60)},{'$set':{'eq':eq,'account':DEMO_ID,'minute':int(time.time()/60),'time':time.time()+300}},upsert=True)

        # for grid collection
        # for grid collection
        # for grid collection
        _account = DEMO_Name
        if _account not in self.account_history:
            self.account_history[_account] = {}
            _last = conn['AccountHistory'][DEMO_ID].find_one({},{'_id':0},sort=[('TradingDay',desc)])
            if _last and _last['TradingDay']==_data['TradingDay']:
                self.account_history[_account][_data['TradingDay']] = _last
            else:
                self.account_history[_account][_data['TradingDay']] = {'o':eq,'h':eq,'l':eq,'c':eq,'TradingDay':_data['TradingDay'],'account':_account}
        if _data['TradingDay'] in self.account_history[_account]:
            if _data['TradingDay']:
                _state = self.db['account'].find_one({'account': DEMO_ID})
                self.account_history[_account][_data['TradingDay']]['h'] = max(self.account_history[_account][_data['TradingDay']]['h'],eq)
                self.account_history[_account][_data['TradingDay']]['l'] = min(self.account_history[_account][_data['TradingDay']]['l'],eq)
                self.account_history[_account][_data['TradingDay']]['c'] = eq
                self.account_history[_account][_data['TradingDay']]['gridp'] = _state.get('gridp',0.0)
                self.account_history[_account][_data['TradingDay']]['grido'] = _state.get('grido',0.0)
                self.account_history[_account][_data['TradingDay']]['_update'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
                conn['AccountHistory'][DEMO_ID].update({'account':_account,'TradingDay':_data['TradingDay']},{'$set':self.account_history[_account][_data['TradingDay']]},upsert=True)
                conn['AccountHistory'][DEMO_ID].delete_many({'TradingDay':{'$lt':str(int(_data['TradingDay'])-10000)}})
        else:
            self.account_history = {}
    def check_account_platten(self,e):
        # for grid collection
        # for grid collection
        # for grid collection
        # for grid collection
        # for grid collection
        # for grid collection
        def do_platten(_account):
            def inTrade():
                _now = dt.datetime.now().strftime('%H%M')
                _int = int(_now)
                if 900 <= _int < 1015:
                    return True
                elif 1030 <= _int < 1130:
                    return True
                elif 1330 <= _int < 1500:
                    return True
                elif 2100 <= _int < 2359:
                    return True
                else:
                    return False

            if not inTrade():
                return 0

            _all = list(conn[DB_NAME]['doubletable'].find({'isTrade': 1, 'master': 1, 'account': _account},
                                                          {'gridlevel': 1, 'gridplatten': 1, 'grid': 1}))
            if len(_all)<=5:
                logger.error('no enough instrument for platten %s'%_account)
                return 0
            _hold = sum([len(a.get('gridplatten', {})) for a in _all])
            _sumup = sum([len(a.get('gridplatten',{}))/grid_level(a.get('gridplatten',{})) for a in _all])
            _ratio = _sumup/len(_all)
            _alleq = list(conn[DB_NAME]['realeq'].find({'account':_account},sort=[('minute',desc)],limit=60))
            _dead = _sumup/len(_all)
            if _alleq:
                _sumeq = 0.0
                _cnteq = 0
                _list = []
                for x in _alleq:
                    if 'r' in x:
                        _list.append(x['r'])
                        _sumeq += x['r']
                        _cnteq += 1
                _maeq = _sumeq/_cnteq
                _stdeq = std(_maeq,_list)
                _dead = 1+(1-(_maeq-_stdeq))
                conn[DB_NAME]['info'].update({'account':_account,'key':'deadline'},{'$set':{'value':_dead,'st':_stdeq,'r':_ratio}},upsert=True)
                conn[DB_NAME]['realeq'].update({'account': _account, 'minute': int(time.time() / 60)}, {'$set': {'r': _ratio,
                                                                                                             'uu':1+(1-_maeq-.05),'nn':1+(1-_maeq+.05),
                                                                                                             'ru':1+(1-_maeq-_stdeq),'rn':1+(1-_maeq+_stdeq),
                                                                                                             'dead':_dead}})
#                return 0
            Account_Double_Count = len(_all) * GRID_DEEP

            '''
            GRID_MOVE = 1
            STEP_LENGTH = 1
            HOURS_RANGE = [9, 10, 11, 13, 14, 21, 22, 23]

            def last_step_profit(d, p):
                _steps = [(x['stop'], x) for x in d.values()]
                _steps.sort()
                if _steps[0][1]['trend'] > 0:
                    _last = _steps[-1][1]
                else:
                    _last = _steps[0][1]
                return int(abs(grid_profit({'0': _last}, p)) + .5)
            '''

            if _hold >= Account_Double_Count and inTrade():
                logger.error('max top %s'%_account)
                _drop = conn[DB_NAME]['doubletable'].find_one(
                    {'master': 1, 'account': _account, 'isTrade': 1, 'bigtimer': {'$gt': time.time() - 600}},
                    sort=[('gridlevel', desc)]) or {}
                if 'gridplatten' in _drop:
                    _dict = _drop['gridplatten']
                else:
                    _dict = _drop.get('grid', {})
                if len(_dict) >= 1:
                    rs = grid_out(_dict, _drop['lastpoint'])
                    _dict = rs['dict']
                    _float = rs['float']
                    # save grid
                    self.get_double_object(_drop['InstrumentID'], self.instInfo).save_platten(_drop['account'], _dict)
                    conn[DB_NAME]['account'].update({'account': _drop['account']},{'$inc': {'gridp': _float + 1}})
                    add_log(
                        u'<font color="red"> << %s <a href="/image/%s/%s/?_=%.0f" target="_blank">%s</a> >> # %d #</font>' \
                        % (_account, _drop['InstrumentID'], _account, time.time(), _drop['InstrumentID'],
                           Account_Double_Count),
                        900)
            elif _hold <= Account_Double_Count * .95 and inTrade():
                logger.error('min top %s'%_account)
                _plus = conn[DB_NAME]['doubletable'].find_one(
                    {'master': 1, 'account': _account, 'isTrade': 1, 'bigtimer': {'$gt': time.time() - 600}},
                    sort=[('gridlevel', asc)]) or {}
                _out = conn[DB_NAME]['account'].find_one({'account': _account}) or {}
                if 'gridplatten' in _plus:
                    _dict = _plus['gridplatten']
                else:
                    _dict = _plus.get('grid', {})
                if len(_dict) >= 1 and _out.get('gridp', 0) > 0:
                    rs = grid_in(_dict, _plus['lastpoint'], 0.0)
                    _dict = rs['dict']
                    # save grid
                    self.get_double_object(_plus['InstrumentID'], self.instInfo).save_platten(_plus['account'], _dict)
                    conn[DB_NAME]['account'].update({'account': _plus['account']}, {'$inc': {'gridp': -1}})
                    add_log(
                        u'<font color="red"> >> %s <a href="/image/%s/%s/?_=%.0f" target="_blank">%s</a> << # %d #</font>' \
                        % (_account, _plus['InstrumentID'], _account, time.time(), _plus['InstrumentID'],
                           Account_Double_Count),
                        900)

            _from = conn[DB_NAME]['doubletable'].find_one({'master': 1, 'account': _account, 'isTrade': 1},sort=[('gridlevel', desc)]) or {}
            _todo = conn[DB_NAME]['doubletable'].find_one({'master': 1, 'account': _account, 'isTrade': 1, 'atop': 0, 'btop': 0}, sort=[('gridlevel', asc)]) or {}
            _plus = conn[DB_NAME]['doubletable'].find_one({'master': 1, 'account': _account, 'isTrade': 1, 'atop': 0, 'btop': 0}, sort=[('gridlevel', asc)],skip=1) or {}
            if _from and _todo and inTrade():
                _max = _from.get('gridlevel', 0)
                _min = _todo.get('gridlevel', 0)

                if _max - _min >= GRID_LEVEL_DELTA:
                    _this_pass = True
                    if 'gridplatten' in _from:
                        _dict = _from['gridplatten']
                    else:
                        _dict = _from.get('grid', {})
                    if _dict:
                        # new grid
                        #                    rs = grid_out(_dict,_from['lastpoint'])
                        # for mini
                        rs = grid_out(_dict, _from['lastpoint'], mini=True)
                        # end for mini
                        _float = rs['float']
                        if 'mini' in rs:
                            _mini = rs['mini']
                        else:
                            _mini = .0
                            _float -= 1.0

                        if _plus:
                            _float -= 1.0

                        _dict = rs['dict']
                        # save grid
                        self.get_double_object(_from['InstrumentID'], self.instInfo).save_platten(_from['account'], _dict)
                        conn[DB_NAME]['doubletable'].update({'InstrumentID': _from['InstrumentID']}, {'$inc': {'move': -2},
                                                                                                      '$set': {'gridlevel': grid_level(_dict)}})

                        if 'gridplatten' in _todo:
                            _dict = _todo['gridplatten']
                        else:
                            _dict = _todo.get('grid', {})
                        if _dict:
                            # new grid
                            #                        rs = grid_in(_dict,_todo['lastpoint'],_float)
                            #                        _dict = rs['dict']
                            # for mini
                            rs = grid_in(_dict, _todo['lastpoint'], _float)
                            _dict = rs['dict']
                            rs = grid_in(_dict, _todo['lastpoint'], _mini)
                            _dict = rs['dict']
                            # end for mini
                            # save grid
                            self.get_double_object(_todo['InstrumentID'], self.instInfo).save_platten(_todo['account'],
                                                                                                      _dict)
                            conn[DB_NAME]['doubletable'].update({'InstrumentID': _todo['InstrumentID']},
                                                                {'$inc': {'move': 2},
                                                                 '$set': {'gridlevel': grid_level(_dict)}})
                            add_log(
                                u'<font color="red"> %s <a href="/image/%s/%s/?_=%.0f" target="_blank">%s</a> < <a href="/image/%s/%s/?_=%.0f" target="_blank">%s</a> %.2f </font>' \
                                % (_account, _todo['InstrumentID'], _account, time.time(), _todo['InstrumentID'],
                                   _from['InstrumentID'], _account, time.time(), _from['InstrumentID'], _float), 900)
                            if _plus and _todo['InstrumentID'] == _plus['InstrumentID']:
                                _plus['gridplatten'] = _dict

                        if _plus:
                            if 'gridplatten' in _plus:
                                _dict = _plus['gridplatten']
                            else:
                                _dict = _plus.get('grid', {})
                            if _dict:
                                # new grid
                                rs = grid_in(_dict, _plus['lastpoint'], .0)
                                _dict = rs['dict']
                                # save grid
                                self.get_double_object(_plus['InstrumentID'], self.instInfo).save_platten(_plus['account'],
                                                                                                          _dict)
                                conn[DB_NAME]['doubletable'].update({'InstrumentID': _plus['InstrumentID']},
                                                                    {'$inc': {'move': 1},
                                                                     '$set': {'gridlevel': grid_level(_dict)}})
                            conn[DB_NAME]['platten'].insert_one(
                                {'move': _float, 'todo': _todo['InstrumentID'], 'plus': _plus['InstrumentID'], \
                                 'symbol': _from['InstrumentID'], 'account': _account, 'date': self.tradingday,
                                 'datetime': dt.datetime.now().strftime('%Y%m%d%H%M%S'), 'time': time.time(), 'platten': 1})
                        else:
                            conn[DB_NAME]['platten'].insert_one({'move': _float, 'todo': _todo['InstrumentID'], \
                                                                 'symbol': _from['InstrumentID'], 'account': _account,
                                                                 'date': self.tradingday,
                                                                 'datetime': dt.datetime.now().strftime('%Y%m%d%H%M%S'),
                                                                 'time': time.time(), 'platten': 1})
            if LOOP_CHECK and _this_pass:
                do_platten(_account)

        all_account = conn[DB_NAME]['account'].find()
        for one in list(all_account):
            th_fork(do_platten,(one['account'],))

        def clear_overplatten():
            conn[DB_NAME]['platten'].delete_many({'time': {'$lt': time.time() - 24 * 3600}})

        th_fork(clear_overplatten, ())
                    # for grid collection
                    # for grid collection
                    # for grid collection

    def onAccount_in(self,event):
        if time.time()-self.tickTimer>5:
            return

        def inTrade():
            _now = dt.datetime.now().strftime('%H%M')
            _int = int(_now)
            if 900 <= _int < 1015:
                return True
            elif 1030 <= _int < 1130:
                return True
            elif 1330 <= _int < 1500:
                return True
            elif 2100 <= _int < 2359:
                return True
            else:
                return False

        def fork_job():
            if inTrade():
                conn[DB_NAME]['realeq'].delete_many({'time':{'$lt':time.time()-3600*24}})
                conn[DB_NAME]['LogicHistory'].delete_many({})
                conn[DB_NAME]['SignalHistory'].delete_many({})

        th_fork(fork_job,())

        _account = event.dict_['_account_']
        _data = event.dict_['data']
        self.tradingday = _data['TradingDay']
        self.tradeClient[_account]['Balance'] = _data['Balance']
        self.tradeClient[_account]['CurrMargin'] = _data['CurrMargin']
        self.account_percent[_account] = _data['CurrMargin']*100/_data['Balance']
        _data['account'] = _account
        _data['_time'] = time.time()
        _data['trade'] = self.tradeClient[_account]['trade']
        _data['_marginshift_'] = self.tradeClient[_account]['marginshift']
        _data['_riskshift_'] = self.tradeClient[_account]['riskshift']
        _data['_begin_'] = self.tradeClient[_account]['begin']
        _data['accupdate'] = dt.datetime.now().strftime('%H%M%S')
        rs = self.db['account'].update({'account':_account},{'$set':_data},upsert=True)
        eq = _data['Balance']
        if inTrade():
            conn[DB_NAME]['realeq'].update({'account':_account,'minute':int(time.time()/60)},{'$set':{'eq':eq,'account':_account,'minute':int(time.time()/60),'time':time.time()+1000}},upsert=True)
        if _account not in self.account_history:
            self.account_history[_account] = {}
            _last = conn['AccountHistory'][_account].find_one({},sort=[('TradingDay',desc)])
            if _last and _last['TradingDay']==_data['TradingDay']:
                self.account_history[_account][_data['TradingDay']] = _last
            else:
                self.account_history[_account][_data['TradingDay']] = {'o':eq,'h':eq,'l':eq,'c':eq,'TradingDay':_data['TradingDay'],'account':_account}
        if _data['TradingDay'] in self.account_history[_account]:
            _state = self.db['account'].find_one({'account': _account})
            self.account_history[_account][_data['TradingDay']]['h'] = max(self.account_history[_account][_data['TradingDay']]['h'],eq)
            self.account_history[_account][_data['TradingDay']]['l'] = min(self.account_history[_account][_data['TradingDay']]['l'],eq)
            self.account_history[_account][_data['TradingDay']]['c'] = eq
            self.account_history[_account][_data['TradingDay']]['gridp'] = _state.get('gridp', 0.0)
            self.account_history[_account][_data['TradingDay']]['grido'] = _state.get('grido', 0.0)
            conn['AccountHistory'][_account].update({'account':_account,'TradingDay':_data['TradingDay']},{'$set':self.account_history[_account][_data['TradingDay']]},upsert=True)
            conn['AccountHistory'][_account].delete_many({'TradingDay':{'$lt':str(int(_data['TradingDay'])-10000)}})
        else:
            self.account_history = {}
    def get_history_days(self,_account,n):
        return list(conn['AccountHistory'][_account].find({},sort=[('TradingDay',desc)],limit=n))
    def subscribe_symbols(self,_account):
        if 1:
            _list = self.master_list
            _acc = [x['md'] for x in self.tickClient.values()]
            _acclen = len(_acc)
            tradecount = 0
            for one in _list:
                self.vol[one['InstrumentID']] = one.get('_vol_',0)
                self.instInfo[one['InstrumentID']] = one
                self.db['symbols'].update({'InstrumentID':one['InstrumentID']},{'$set':{'_lastvol_':one.get('_vol_',0),\
                '_outdate_':int(one['ExpireDate'])}},upsert=True)
                if one['_master_']==2:
                    self.master[one['InstrumentID']] = one
                    self.masterPd[one['ProductID']] = one
                    tradecount += 1
#====================================================
#====================================================
            if 1:
                n = 0
                while n<len(_list):
                    _symbol = _list[n]['InstrumentID']
                    _exchange = _list[n]['ExchangeID']
                    _product = _list[n]['ProductID']
                    _master = _list[n]['_master_']
                    _vol = _list[n].get('_vol_',0)
                    if _product not in Skip_Product:
                        _acc[n%_acclen].subscribe(str(_symbol),str(_product),str(_exchange),_master)
                    n += 1
                event = Event(type_=EVENT_LOG)
                log = u'%s 订阅合约: %d 个 交易合约: %d 个'%(_account,n,tradecount)
                event.dict_['log'] = log
                self.ee.put(event)
        logger.error('subinst %s'%_account)
    def tick2logic(self,event):
        _data = event.dict_['data']
        _inst = _data['InstrumentID']
        if int(_data['TradingDay']) > int(self.tradingday):
            self.tradingday = _data['TradingDay']
            for _acc,_tick in self.tickClient.items():
                _tick['md'].set_tradingday( _data['TradingDay'] )
                add_log(u'推送 %s TradingDay %s'%(_acc,_data['TradingDay']),0)
        self.tickTimer = time.time()
        self.tickCache[_inst] = _data
        o = self.get_inst_object(_inst)
        o.new_price(time.time(),_data)
        o.get_trade()
    def get_error(self,event):
        _data = event.dict_
        if _data['ErrorID'] not in [5,7,21,30,50,51,90]:
            _data['time'] = time.time()
            self.db['error'].insert(_data)
            self.db['error'].delete_many({'time':{'$lt':time.time()-3600*24*25}})
    def get_log(self,event):
        _data = event.dict_
        _data['time'] = time.time()
        self.db['error'].insert(_data)
        self.db['error'].delete_many({'time':{'$lt':time.time()-3600*24*30}})
    def save4askbid(self,event):
        return 0
        _data = event.dict_['data']
        _inst = _data['InstrumentID']
        if _inst in self.instCache and _inst in self.master:
            _product = self.instCache[_inst].info['ProductID']
            _last = self.db['AskBid'].find_one({'ProductID':_product},{'_id':0},sort=[('TradingDay',desc)])
            if _last:
                if _last['TradingDay'] == _data['TradingDay']:
                    _last['AskSum'] = _last['AskSum']+_data['AskVolume1']
                    _last['BidSum'] = _last['BidSum']+_data['BidVolume1']
                    _last['AskCnt'] = _last['AskCnt']+1
                    _last['BidCnt'] = _last['BidCnt']+1
                    _last['AskLevel'] = _last['AskSum']/_last['AskCnt']
                    _last['BidLevel'] = _last['BidSum']/_last['BidCnt']
                    self.db['AskBid'].update({'ProductID':_product,'TradingDay':_last['TradingDay']},{'$set':_last},upsert=True)
                    if _last['AskCnt']%1000==0:
                        logger.error(str(('ctpengine.save4askbid:',_last)))
                else:
                    _last['TradingDay'] = _data['TradingDay']
                    _last['AskSum'] = _data['AskVolume1']
                    _last['BidSum'] = _data['BidVolume1']
                    _last['AskCnt'] = 1
                    _last['BidCnt'] = 1
                    _last['AskLevel'] = _last['AskSum']/_last['AskCnt']
                    _last['BidLevel'] = _last['BidSum']/_last['BidCnt']
                    self.db['AskBid'].update({'ProductID':_product,'TradingDay':_last['TradingDay']},{'$set':_last},upsert=True)
                    self.db['AskBid'].delete_many({'TradingDay':{'$lt':str(int(_last['TradingDay'])-10000)}})# one year
                    logger.error(str(('ctpengine.save4askbid daybegin :',_last)))
            else:
                if 1:
                    _last = {}
                    _last['TradingDay'] = _data['TradingDay']
                    _last['AskSum'] = _data['AskVolume1']
                    _last['BidSum'] = _data['BidVolume1']
                    _last['AskCnt'] = 1
                    _last['BidCnt'] = 1
                    _last['AskLevel'] = _last['AskSum']/_last['AskCnt']
                    _last['BidLevel'] = _last['BidSum']/_last['BidCnt']
                    self.db['AskBid'].update({'ProductID':_product,'TradingDay':_last['TradingDay']},{'$set':_last},upsert=True)
    def subscribe(self, instrumentid, exchangeid):
        """订阅合约"""
        self.md.subscribe(str(instrumentid), str(exchangeid))
    #----------------------------------------------------------------------
    def save4vol(self,event):
        pass
    def ready_subscribe_single(self,event):
        if 1:
            sleep(1)
            _account = event.dict_['_account_']
            if 1:
                event = Event(type_=EVENT_LOG)
                log = u'<font color="green">行情账户 %s 登录成功</font>'%_account
                event.dict_['log'] = log
                self.ee.put(event)
            self.subscribe_symbols(_account)
            self.rebootCount['md%s'%_account] = 1
    def ready_trade_single(self,event):
        if 1:
            sleep(2)
            _account = event.dict_['_account_']
            if 1:
                event = Event(type_=EVENT_LOG)
                log = u'<font color="green">交易账户 %s 登录成功</font>'%_account
                event.dict_['log'] = log
                self.ee.put(event)
            _td = self.tradeClient[_account]['td']
            self.rebootCount['td%s'%_account] = 1
            _td.getInstrument()
            logger.error('getInstrument @ %s'%_account)
            sleep(5)
            self.query_circle_in(event)
    def query_circle_in(self, event):
        """循环查询账户和持仓"""
        if '_account_' not in event.dict_:return
        _account = event.dict_['_account_']
        if self.ended:return
        if _account not in self.tradeClient:return
        # 每1秒发一次查询
        if self.tradeClient[_account]['lastGet'] == 'Account':
            self.tradeClient[_account]['lastGet'] = 'Position'
            if 'td' in self.tradeClient.get(_account,{}):
                self.tradeClient[_account]['td'].getPosition()
        else:
            self.tradeClient[_account]['lastGet'] = 'Account'
            if 'td' in self.tradeClient.get(_account,{}):
                self.tradeClient[_account]['td'].getAccount()
    def timerSaveSymbolInfo(self,event):
        if self.symbolInfoCache:
            one = self.symbolInfoCache.pop(self.symbolInfoCache.keys()[0])
            _inst = one['InstrumentID']
            _product = one['ProductID']
            _exchange = one['ExchangeID']
            if _exchange != 'CFFEX':
                one['ExpireDate'] = expiredate_shift(one['ExpireDate'])
            if self.db['symbols'].find({'InstrumentID':_inst}).count()==0:
                one['_outdate_'] = int(one['ExpireDate'])
                one['_lastday'] = '0'
                one['_lastvol_'] = 0
                one['_vol_'] = 0
                one['time'] = time.time()
                one['_master_'] = 0
            one['_update_'] = dt.datetime.now().strftime('%Y%m%d:%H%M%S')
            if one['ProductID'] not in Skip_Product:
                self.db['symbols'].update({'InstrumentID':_inst},{'$set':one},upsert=True)
#            logger.error('clear symbolInfoCache %s %s %s %d'%(_inst,_product,_exchange,len(self.symbolInfoCache)))
            def do_job():
                if not self.cleaning:
                    self.cleaning = True
                    try:
                        _todel = self.db['symbols'].find({'_outdate_':{'$lt':-40+int(dt.datetime.now().strftime('%Y%m%d'))}})
                        for _del in list(_todel)[:10]:
                            self.db['symbols'].delete_many({'InstrumentID':_del['InstrumentID']})
                            self.db['table'].delete_many({'InstrumentID':_del['InstrumentID']})
                            self.db['doubleid'].delete_many({'a':_del['InstrumentID']})
                            self.db['doubleid'].delete_many({'b':_del['InstrumentID']})
#                            self.db['doubletable'].delete_many({'a':_del['InstrumentID']})
#                            self.db['doubletable'].delete_many({'b':_del['InstrumentID']})
                            self.db['doubleid'].delete_many({'time':{'$lt':time.time()-365*24*3600}})
                            self.db['doublek'].delete_many({'a':_del['InstrumentID']})
                            self.db['doublek'].delete_many({'b':_del['InstrumentID']})
                            self.db['doublek'].delete_many({'time':{'$lt':time.time()-365*24*3600}})
                            self.db['state'].delete_many({'a':_del['InstrumentID']})
                            self.db['state'].delete_many({'b':_del['InstrumentID']})
                            self.db['state'].delete_many({'time':{'$lt':time.time()-365*24*3600}})
                            self.db['oldMaster'].delete_many({'InstrumentID':_del['InstrumentID']})
                            conn.drop_database(_del['InstrumentID'])
                            logger.error('drop_database %s'%_del['InstrumentID'])
                            event = Event(type_=EVENT_LOG)
                            log = u'清除过期合约数据# %s'%_del['InstrumentID']
                            event.dict_['log'] = log
                            self.ee.put(event)
                        self.db['doubletable'].delete_many({'overdate':{'$lt':dt.datetime.now().strftime('%Y%m%d')}})
                    finally:
                        self.cleaning = False
            th_fork(do_job,())

    def save_instrument_to_db(self,event):
        data = event.dict_['data']
        if data['IsTrading']>0:
            _inst = data['InstrumentID']
            self.symbolInfoCache[_inst] = data
        if event.dict_['last']:
            add_log(u'共收到%d合约'%len(self.symbolInfoCache),300)
    #----------------------------------------------------------------------
    def login(self):
        """登陆"""
        print("me.login")
        self.td.login()
        self.md.login()

    #----------------------------------------------------------------------
    def getAccount(self):
        """查询账户"""
        self.td.getAccount()

    #----------------------------------------------------------------------
    def getInvestor(self):
        """查询投资者"""
        self.td.getInvestor()

    #----------------------------------------------------------------------
    def getPosition(self):
        """查询持仓"""
        self.td.getPosition()

    #----------------------------------------------------------------------
    def exitEvent(self,e):
        self = None
    def exit(self):
        """退出"""
        # 销毁API对象
        print('====== ctpengine exit ======')#%self.init_date)
        for one in self.tickClient.values():
            one['ee'].stop()
        for one in self.tradeClient.values():
            one['ee'].stop()

        # 停止事件驱动引擎
        self.ee.stop()

    def __del__(self):
        self.exit()
