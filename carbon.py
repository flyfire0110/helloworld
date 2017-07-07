#coding:utf-8
import datetime
from log import *
from life import *
import threading
from svgcandle import *
print 'core init01'
import thread
import urllib,urllib2
import json
import time
from copy import copy
from thread import start_new_thread as th_fork
import traceback
from settings_ctp import DOUBLE_RUN_SET,DB_NAME,core_vsn
from settings_account import *
from settings_risk_controller import *
from ctp_data_type import defineDict
from trade_api import *
from math import e as math_e
from math import log as math_log
from math import log10
from math import sqrt as math_sqrt
from mmgg import *

print 'core init'

def money_limit(n): # w
    a = log10(n)
    b = a/3
    c = n*.05
    return max(5,2.0*c*b)

def expiredate_shift(date,_days = 30):
    d = datetime.datetime.strptime(date,'%Y%m%d')
    nd = d - datetime.timedelta(days=_days)
    return nd.strftime('%Y%m%d')

def risk_lot(_price,_value_of_point,_money,step = 5,maxstep = 8):
    p1 = math_log(_price)
    p2 = math_e**(p1+step*1.0/Point_Multi)
    pp = abs(p2-p1)*_value_of_point*sum(range(1,1+maxstep))
    return int(_money/pp)

def get_margin(_margin,thisid = -1,allid = -1):
    if thisid>0 and allid>0:
        _money = _margin*thisid*1.0/allid
    else:
        _money = _margin*Margin_Percent/1000
    return (_money,-1)

def get_margin_pk(_margin,_pk):
    if _pk>=0:
        _money = _margin*_pk/100.0
    else:
        _money = 0.0
    return (_money,-1)

def get_lot(*a,**b):
    return (0,0)

def time2datetime(n):
    return datetime.datetime.fromtimestamp(n)
#======================================================================

# ======================================================================
Wait_Result = 3

class Couple:
    def __init__(self,symbol,infoDict,dbConnection,trade=isTrade):
        self.lastn = {}
        self.init_time = time.time()
        self.conn_ = dbConnection
        self.trade = trade
        self.symbol = symbol
        self.logic = 'couple'
        self.name = symbol
        _inst_list = symbol.split('_')
        self.info_dict = {}
        self.inst_list = _inst_list
        self.first = self.inst_list[0]
        self.secend = self.inst_list[1]
        self.aa = self.secend+'_%da'
        self.bb = self.secend+'_%db'
        self.db = self.conn_[self.first]
        self.center = self.conn_[DB_NAME]
        self.center['doubletable'].update({'InstrumentID':self.symbol},{'$set':{'a':self.first,'b':self.secend,'atop':0,'btop':0}},multi=True)
        self.doublek = self.center['doublek']
        self.savek = self.db[self.secend]
#        self.saveprofit = self.db[self.secend+'pf']
        self.table = self.center['doubletable']
        self.savek.create_index([('group',desc),('n',desc)],background=True)
#        self.saveprofit.create_index([('n',desc)],background=True)
        self.master = 0
        self.overdate = '99999999'
        self.noTrade = False
        self.master1 = -1
        self.master2 = -1
        self.overdate1 = '00000000'
        self.overdate2 = '00000000'
        if infoDict:
            import datetime
            for k in _inst_list:
                self.info_dict[k] = infoDict[k]
            self.overdate = min([x['ExpireDate'] for x in self.info_dict.values()])
            self.pd1 = self.info_dict[self.first ]['ProductID']
            self.pd2 = self.info_dict[self.secend]['ProductID']
            self.product = '%s_%s'%(self.pd1,self.pd2)
            _days = datetime.datetime.strptime(self.overdate,'%Y%m%d')-datetime.datetime.now()
            _day_delta = datetime.datetime.strptime(max([x['ExpireDate'] for x in self.info_dict.values()]),'%Y%m%d')-datetime.datetime.strptime(min([x['ExpireDate'] for x in self.info_dict.values()]),'%Y%m%d')
            self.vol = min([x.get('_vol_',0.1) for x in self.info_dict.values()])
            self.voldate = int(math_log(max(1,self.vol*self.vol*_days.days/max(1,_day_delta.days)))*100)
            self.master1 = self.info_dict[self.first ]['_master_']
            self.master2 = self.info_dict[self.secend]['_master_']
            def change_group(_p):
                if _p<75:return 3
                elif _p>150:return 1
                else:return 2
            overdate1 = self.info_dict[self.first]['ExpireDate']
            self.overdate1 = overdate1
            overdate2 = self.info_dict[self.secend]['ExpireDate']
            self.overdate2 = overdate2
            over1 = int(365/max(2,len(list(self.center['symbols'].find({'ProductID':self.pd1})))))
            over2 = int(365/max(2,len(list(self.center['symbols'].find({'ProductID':self.pd2})))))
            if change_group(over1)>=change_group(over2):
                overdate = overdate1
                seedate = overdate2
                over = over2
            else:
                overdate = overdate2
                seedate = overdate1
                over = over1
            _date = datetime.datetime.strptime(overdate,'%Y%m%d')
            maxdate = _date + datetime.timedelta(days=over)
            mindate = _date - datetime.timedelta(days=over)
            _master = self.center['doubleMaster'].find_one({'overdate':{'$gt':datetime.datetime.now().strftime('%Y%m%d%H%M%S')},'product':self.product}) or {}
            if mindate.strftime('%Y%m%d')<=seedate<=maxdate.strftime('%Y%m%d'):
                pass
            else:
                self.noTrade = True
                self.voldate = -8888
            self.center['doubleid'].update({'symbol':self.symbol},{'$set':{'time':time.time(),'update':datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),'product':self.product,'vol':self.vol,'voldate':self.voldate}})
            self.center['doubleMaster'].update({'symbol':self.symbol},{'$set':{'overdate':(overdate1,overdate2)}})
        self.bridge = 34.0
        self.profitratio = 0.05
        self.price = -1
        self.step = 8.0
        self.maxstep = 8
        self.tick_time = 0
        #=====================================================================
        self.UpdateTime = 0
        self.SkipTick = 0
        self.version = core_vsn + self.symbol + self.logic
        self.todo = [3,11]
#        self.todo = [3,5,7,9,11]
        self.todo_timeframes = {3:1,5:2,7:4,9:24,11:24}
        self.image = {'see':-1}
        self.check_old = False
        self.timeshift = -5*3600
        self.hour = 0
        self.pos = 0
        self.tradingday = '00000000'
        self.last_day = '0'
        self.last_volume = -1
        self.trade_accounts = {}
        self.cache = {}
        self.rtnAccountTradeO = {}
        self.rtnAccountTradeC = {}
#        logger.error(u'<font style="color:blue">初始化对冲品种 %s %.3f</font>'%(self.symbol,time.time()-self.init_time))
    def setMaster(self,infoDict):
        _haved = self.center['doubleMaster'].find_one({'product':"%s_%s"%(self.pd1,self.pd2)}) or {}
        self.master = 0
        if _haved:
            if _haved['symbol'] == self.symbol:
                self.master = 1
            else:
                _list = _haved['symbol'].split('_')
#                if self.product in ['al_pb','cu_pb']:
#                    add_log('check master %s %s'%(self.symbol,str((self.overdate1,self.overdate2,_haved['overdate']))),3600)
                if self.overdate1>=_haved['overdate'][0] \
                    and self.overdate2>=_haved['overdate'][1]:
                    _h = self.center['doubleid'].find_one({'product':self.product},sort=[('voldate',desc),('vol',desc)])
                    _o = self.center['doubleid'].find_one({'symbol':_haved['symbol']})
#                    if self.product in ['al_pb','cu_pb']:
#                        add_log('check master newer %s'%self.symbol,3600)
                    if _h and _h['symbol'] == self.symbol and _o['voldate']<=2000:
                        self.master = 1
                        #   clear table
                        self.conn_[_list[0]]['double'].delete_many({'InstrumentID':_haved['symbol']})
                        self.conn_[_list[1]]['double'].delete_many({'InstrumentID':_haved['symbol']})
                        #   update master
                        self.table.delete_many({'InstrumentID':_haved['symbol']})
                        self.center['doubleMaster'].update({'_id':_haved['_id']},{'$set':{'voldate':_h['voldate'],'marginall':0,'overdate':(self.overdate1,self.overdate2),'before':_haved['symbol'],'date':datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),'symbol':self.symbol,'product':"%s_%s"%(self.pd1,self.pd2)}},upsert=True)
                        add_log(u'<font color="red">主力合约换月 %s => %s</font>'%(_haved['symbol'],self.symbol),3600)
        elif self.master1+self.master2==4:
            self.center['doubleMaster'].update({'product':"%s_%s"%(self.pd1,self.pd2)},{'$set':{'overdate':(self.overdate1,self.overdate2),'date':datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),'symbol':self.symbol,'product':"%s_%s"%(self.pd1,self.pd2)}},upsert=True)
            self.master = 1
            add_log(u'<font color="red">主力合约初始化 %s</font>'%self.symbol,3600)
        return self.master
    def price2point(self,price):
        return price*Point_Multi
    def point2price(self,price):
        return price/Point_Multi
    def range_k(self):
        for one in self.todo:
            self.pos_ = one
            self.do_price(self.bb%one,self.point,one)
            self.do_price(self.aa%one,self.point,one)
    def save_k(self,name_,_data):
        def save_h(name_,key_,data_):
            key_['group'] = name_
            key_['symbol'] = self.symbol
            data_['group'] = name_
            data_['symbol'] = self.symbol
            data_['a'] = self.first
            data_['b'] = self.secend
            if name_[-1] == 'a':
                d = {}
                for k,v in data_.items():
                    if type(v)==type(0.1):
                        d[k] = v
                    elif type(v)==type(1):
                        d[k] = v
                    elif type(v)==type(''):
                        d[k] = v
                    elif type(v)==type({}):
                        d[k] = v
                self.savek.update(key_,{'$set':d},upsert=True)
                if data_['n']%10==0 and data_.get('clearhistory',0)==0:
                    data_['clearhistory'] = 1
                    rs = self.savek.delete_many({'n':{'$lt':data_['n']-100},'symbol':self.symbol,'group':name_})
            self.doublek.update(key_,{'$set':data_},upsert=True)
            if data_['n']%2==0:
                self.doublek.delete_many({'symbol':self.symbol,'group':name_,'n':{'$lt':data_['n']-5}})
        data_ = _data
        _key = {'n':data_['n']}
        if data_['do']>0:
            save_h(name_,_key,data_)
    def get_result(self,name=''):
        c = self.cache
        def just(uu,nn,u,n):
            a = max(0,uu-u)
            b = max(0,n-nn)
            a = uu-u
            b = n-nn
            return -100*(a-b)/max(1,a+b)
        pos = self.pos_
        aa = self.aa%pos
        bb = self.bb%pos

        if aa not in c:
            c[aa] = list(self.doublek.find({'group':aa,'symbol':self.symbol,'do':1},{'_id':0},sort=[('n',desc)],limit=2))
            logger.error('@ cache data from DB %s %s'%(self.symbol,aa))
        if bb not in c:
            c[bb] = list(self.doublek.find({'group':bb,'symbol':self.symbol,'do':1},{'_id':0},sort=[('n',desc)],limit=2))
            logger.error('@ cache data from DB %s %s'%(self.symbol,bb))

        if aa in c:
            if pos == self.todo[-1] and c[aa][0]['n'] != self.lastn.get(pos,0):
                self.lastn[pos] = c[aa][0]['n']
                self.center['doubleid'].update({'symbol':self.symbol},{'$set':{'maxn':self.lastn[pos]}})


        k1u = max([border(c[bb][0], 1,x,'A',p=1) for x in [3,4,5,6,7]])
        k1n = min([border(c[bb][0],-1,x,'A',p=1) for x in [3,4,5,6,7]])
        k1up = max([border(c[bb][0], 1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k1np = min([border(c[bb][0],-1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k8u = max([border(c[aa][0], 1,x,'A',p=1) for x in [3,4,5,6,7]])
        k8n = min([border(c[aa][0],-1,x,'A',p=1) for x in [3,4,5,6,7]])
        k8up = max([border(c[aa][0], 1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k8np = min([border(c[aa][0],-1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k8o = border(c[aa][0],0,7,'A',p=0)

        raw = sss = just(k8o+pos*(1-myth)*10,k8o-pos*(1-myth)*10,k1u,k1n)
        slevel = sun = k1up+k1np
        if sun>0:
            s = self.state.get(str(pos),{'long':-1,'short':-1,'ls':-1,'price':self.price,'point':self.point})
        else:
            s = self.state.get(str(pos),{'long':1,'short':1,'ls':1,'price':self.price,'point':self.point})

        kkk = abs(slevel)
        _kun = k1up+k1np
        wu = -1*k1np
        wn = -1*k1up
        wu =  border(c[aa][0], 1,7,'A',p=1)
        wn =  border(c[aa][0],-1,7,'A',p=1)
        bu = k8u
        bn = k8n

        self.state['center'] = k8o
        self.state['sun'] = slevel
        self.save_state()

        self.state['w%d'%pos] = k8u-k8n
        s['Alevel'] = slevel
        s['bu'] = bu
        s['bn'] = bn
        s['wu'] = wu
        s['wn'] = wn

        for one in [aa]:
            c[one][0]['kun'] = raw
            c[one][0]['sun'] = min(110,max(-110,slevel))
            c[one][0]['run'] = sun
            c[one][0]['bu'] = bu
            c[one][0]['bn'] = bn
            c[one][0]['k1up'] = k1up
            c[one][0]['k1np'] = k1np
            c[one][0]['k8up'] = k8up
            c[one][0]['k8np'] = k8np
            c[one][0]['k8ua'] = k8u
            c[one][0]['k8na'] = k8n
            c[one][0]['wu'] = wu
            c[one][0]['wn'] = wn
            self.save_k(one,c[one][0])


        Short = s['short']
        Long = s['long']
        LS = s['ls']
        Price = s['price']
        Point = s['point']

        if Short==0 and s.get('tradepos',0) != c[bb][0]['n']:
            if 1:
                if Long<=0 and bu>wu and sun>c[aa][1].get('run',0):
                    s['short'] = Short = 1
                    s['long'] = Long = 1
                if Long>=0 and bn<wn and sun<c[aa][1].get('run',0):
                    s['short'] = Short = -1
                    s['long'] = Long = -1
            if 1:# DON'T CHANGE HERE
                if Short==0 and bu>wu and sun>c[aa][1].get('run',0):
                    s['long'] = Long = 1
                    s['short'] = Short = 1
                if Short==0 and bn<wn and sun<c[aa][1].get('run',0):
                    s['short'] = Short = -1
                    s['long'] = Long = -1
        elif s.get('tradepos',0) != c[bb][0]['n']:
            if 1:
                if Long<=0 and bu>wu and sun>c[aa][1].get('run',0):
                    s['short'] = Short = 1
                    s['long'] = Long = 1
                if Long>=0 and bn<wn and sun<c[aa][1].get('run',0):
                    s['short'] = Short = -1
                    s['long'] = Long = -1
            if Short>0 and bu<=wu and sun<c[aa][1].get('run',0):
                s['short'] = Short = 0
            if Short<0 and bn>=wn and sun>c[aa][1].get('run',0):
                s['short'] = Short = 0


        if type(self.state['result']) != type({}):
            self.state['result'] = {}
            self.save_state()

        LS2 = Short

        if LS2!=LS:
            self.SkipTick = time.time()+Wait_Result
            s['ls'] = LS2
            _profit = LS*(self.price-Price)
            s['tradeid'] = s.get('tradeid',0)+1
            s['price'] = self.price
            s['tradetime'] = time2datetime(self.timer).strftime('%Y%m%d %H%M%S')
            s['profit'] = _profit
            s['point'] = Point = c[bb][0]['c']
            s['tradepos'] = c[bb][0]['n']
            s['result'] = LS2
            c[aa][0]['point'] = Point
            c[aa][0]['result'] = LS2
            c[aa][0]['gridlevel'] = self.state.get('gridlevel',0)
            self.save_k(aa,c[aa][0])
            self.state[str(pos)] = s
            self.save_state()

        if Long!=Short:
            LS2 = Long
        else:
            LS2 = Short

        self.state['result'][str(pos)] = (LS2,k8up-k8np)
        self.save_state()
    def new_price(self,timer,Price,TradingDay):
        if 1:
            self.timer = timer
            self.tick_time = time.time()
            self.tradingday = TradingDay
            self.price = Price
            self.point = self.price2point(self.price)
            self.range_k()
        return 0
    def check_k_period(self,now,last,name,timeframe):
        if last.get('n',0)<10:
            timeframe = 60
        _hour = int(self.timer+self.timeshift)/timeframe
        if now.get('hour',0)!=_hour:
            p = now['c']
            new = {'o':p,'h':p,'l':p,'c':p,'do':0,'hour':_hour}
            now = self.check_base(name,now,last)
            return (new,now)
        return (now,last)
    def check_k_len(self,now,last,name,len_,n=0):
        if name==self.bb%len_:
            length = 5#self.state.get('w%d'%len_,0)/self.bridge or 1
        else:
            length = now['len'] = len_
        if now['h']-now['o']>length and n<100:
            high = now['h']
            now['h'] = now['o']+length
            now['c'] = now['o']+length
            new = {'o':now['c'],'h':high,'l':now['c'],'c':now['c'],'do':0,'hour':now['hour']}
            now = self.check_base(name,now,last)
            return self.check_k_len(new,now,name,len_,n=n+1)
        elif now['o']-now['l']>length and n<100:
            low = now['l']
            now['l'] = now['o']-length
            now['c'] = now['o']-length
            new = {'o':now['c'],'h':now['c'],'l':low,'c':now['c'],'do':0,'hour':now['hour']}
            now = self.check_base(name,now,last)
            return self.check_k_len(new,now,name,len_,n=n+1)
        else:
            return (now,last)
    def do_price(self,name_,point_,len_):
        self.pos_ = len_
        if 1<0 and len(self.cache.get(name_,{}))==2:
            _result = self.cache[name_]
        else:
            _result = list(self.doublek.find({'group':name_,'symbol':self.symbol},{'_id':0},sort=[('n',desc)],limit=2))
        if len(_result)>0:
            now = _result[0]
            if len(_result)>1:
                last = _result[1]
            else:
                last = {}

            now['c'] = point_
            if point_>now['h'] or point_<now['l'] or time.time()-now.get('time',0)>60:
                now['h'] = max(now['c'],now['h'])
                now['l'] = min(now['c'],now['l'])
                now['pos'] = len_
                now['do'] = 0
                now,last = self.check_k_len(now,last,name_,len_)
                now,last = self.check_k_period(now,last,name_,self.todo_timeframes[len_]*3600)
                self.check_base(name_,now,last)
            elif last:
                return 0
            else:
                return 0
        else:
            if 1:
                _point = self.point
                last = {}
                now = {'do':0,'o':point_,'h':point_,'l':point_,'c':point_,'hour':0,'point':point_,'n':0}
                now['pos'] = len_
                self.check_base(name_,now,last)
    def check_base(self,_name,_todo,_last):
        _todo,_last = fill_base(_todo,_last,_flow='ABC')
        _todo['do'] = 1
        _todo['ll'] = _todo['h']-_todo['l']
        _todo['time'] = time.time()
        _todo['tradingday'] = datetime.datetime.now().strftime('%Y%m%d')
        if _last:
            _todo['n'] = _last.get('n',0)+1
            _todo['result'] = self.state['result'].get(str(self.pos_),(0,0))[0]
            _todo['tradingday'] = self.tradingday
            self.cache[_name] = [_todo,_last]
            if _name[-1] == 'a':
                self.get_result(name=_name)
            self.save_k(_name,_todo)
        else:
            _todo['n'] = 1
            self.cache[_name] = [_todo]
            self.save_k(_name,_todo)
        return _todo
    def get_inst_couple_info(self,tickCache):
        _first = {}
        _first['symbol'] = _inst = self.inst_list[0]
        _first['tick'] = _t = tickCache[_inst]
        _first['price'] = _t['LastPrice']
        _first['marginradio'] = {}
        _first['marginradio'][ 1] = self.info_dict[_inst]['LongMarginRatio']
        _first['marginradio'][-1] = self.info_dict[_inst]['ShortMarginRatio']
        _first['pointvalue'] = self.info_dict[_inst]['VolumeMultiple']
        _first['exchangeid'] = self.info_dict[_inst]['ExchangeID']
        _first['productid'] = self.info_dict[_inst]['ProductID']
        _first['pk'] = _pk1 = max(_first['marginradio'].values())
        _secnd = {}
        _secnd['symbol'] = _inst = self.inst_list[1]
        _secnd['tick'] = _t = tickCache[_inst]
        _secnd['price'] = _t['LastPrice']
        _secnd['marginradio'] = {}
        _secnd['marginradio'][ 1] = self.info_dict[_inst]['LongMarginRatio']
        _secnd['marginradio'][-1] = self.info_dict[_inst]['ShortMarginRatio']
        _secnd['pointvalue'] = self.info_dict[_inst]['VolumeMultiple']
        _secnd['exchangeid'] = self.info_dict[_inst]['ExchangeID']
        _secnd['productid'] = self.info_dict[_inst]['ProductID']
        _secnd['pk'] = _pk2 = max(_secnd['marginradio'].values())
        return (_first,_secnd)
    def add_one_platten(self,acc_,grid_,point_,update={}):
        _grid = grid_platten_one(grid_,point_,update=update)
        self.state[acc_]['plattengrid'] = _grid
        self.table.update({'InstrumentID':self.symbol,'account':acc_},{'$set':{'gridplatten':_grid,'gridlevel':grid_level(_grid),'platten_in_time':datetime.datetime.now().strftime('%H%M%S')}})
        self.save_state()
    def do_platten(self,acc):
        if 'plattengrid' in self.state[acc]:
            _state = self.state[acc]
            grid_ = _state.pop('plattengrid')
            _state['grid'] = grid_
            _plus = [x['trend']*abs(x['id']) for x in _state['grid'].values()]
            self.table.update({'InstrumentID':self.symbol,'account':acc},{'$set':{'platten_do_time':datetime.datetime.now().strftime('%H%M%S'),'change':time.time(),'grid':grid_,'plus':_plus}},upsert=True)
            return _state
        return {}
    def save_platten(self,acc_,grid_):
        self.state[acc_]['plattengrid'] = grid_
        self.table.update({'InstrumentID':self.symbol,'account':acc_},{'$set':{'cnt':0,'gridplatten':grid_,'gridlevel':grid_level(grid_),'platten_in_time':datetime.datetime.now().strftime('%H%M%S')}})
        self.save_state()
    def get_trade(self,tickCache):
        def grid_profit(d,p):
            s = 0.0
            for k,v in d.items():
                s += (p-v['open'])*v['trend']
            return s/8.0
        self.trade_time = time.time()
        if self.master<1:
            return 0
        if not self.trade_accounts:
            logger.error('no trade_accounts @ couple.get_trade')
            return 0
        _show = self.aa
        _first,_secnd = self.get_inst_couple_info(tickCache)
        _pk1 = _first['pk']
        _pk2 = _secnd['pk']
        self.step = 8.0
        self.maxstep = 8
        _result = self.state['result']
        _sum = sum([x[1] for x in _result.values()])
        _center = self.state['center']
        trade_str = {0:u'',1:u'*'}
        for one in self.trade_accounts.values():
            _account = one['account']
            if _account not in self.state:
                self.state[_account] = {'isTrade':0,'pk':0.0,'grid':{},'lastpoint':0,'lock':0,'gridid':0,'lastid':0,'thisww':0,'trade_status_version':()}
            if 'grid' not in self.state[_account]:
                self.state[_account] = {'isTrade':0,'pk':0.0,'grid':{},'lastpoint':0,'lock':0,'gridid':0,'lastid':0,'thisww':0,'trade_status_version':()}
            #============================
            _state = self.do_platten(_account)
            if _state:
                self.state[_account] = _state
#                self.state[_account]['lot1'] = sum([x['lot1'] for x in self.state[_account]['grid'].values()])
#                self.state[_account]['lot2'] = sum([x['lot2'] for x in self.state[_account]['grid'].values()])
                self.save_state()
            #============================
            _ms = one.get('marginshift',{'a':0,'b':1,'c':0})
            _margin = (one['Balance']+_ms['a'])*_ms['b']+_ms['c']
            _allmargin = get_margin_pk(_margin,self.state[_account].get('pk',0.0))
            _marginall = self.maxstep*(_first['price']*_first['pointvalue']*_first['marginradio'][1]+_secnd['price']*_secnd['pointvalue']*_secnd['marginradio'][1])
            _trade_str = trade_str[self.state[_account].get('isTrade',0)]
            _pos = -1
            if self.state['result'][str(self.todo[_pos])][0]!=0:
                if self.state[_account].get('lastlock',0) != self.state['result'][str(self.todo[_pos])][0]:
                    self.state[_account]['lastlock'] = self.state['result'][str(self.todo[_pos])][0]
                    self.save_state()
            _sum = self.state['result'][str(self.todo[_pos])][0]
            _big = self.state['result'][str(self.todo[_pos])][1]
            if _sum == 0:
                if self.state[_account].get('lock',0)>0:
                    self.table.update({'InstrumentID':self.symbol,'account':_account},{'$set':{'change':time.time()}})
                    self.state[_account]['lock'] = 0
                    self.save_state()
                _grid_type = 0
            else:
                if self.state[_account].get('lock',0)==0:
                    self.table.update({'InstrumentID':self.symbol,'account':_account},{'$set':{'change':time.time()}})
                    self.state[_account]['lock'] = 1
                    self.save_state()
                _grid_type = int(_sum/abs(_sum))

            #============================
            _stepmargin = _allmargin[0]/self.maxstep
            _first_price = _first['tick']['LastPrice']
            self.state[_account]['step'] = self.step
            self.state[_account]['stpf'] = _stpf = _margin*abs(math_e**(self.step/Point_Multi+math_log(_first_price))-_first_price)/(_first_price*(_pk1+_pk2)*len(DOUBLE_RUN_SET)*self.maxstep)
            self.state[_account]['pfmr'] = _pfmr = _stpf*len(DOUBLE_RUN_SET)*self.maxstep/_margin
            _update = {'time':time.time(),'date':datetime.datetime.now().strftime('%Y%m%d%H%M%S'),'price':self.price,'lot':1}
            _update['price1'] = _first['price']
            _update['price2'] = _secnd['price']
            _update['pf'] = _pfmr*100
            #============================
            _update['lot1'] = _stepmargin * _pk1 / (
            (_pk1 + _pk2) * _first['price'] * _first['marginradio'][1] * _first['pointvalue'])
            _update['lot2'] = _stepmargin * _pk2 / (
            (_pk1 + _pk2) * _secnd['price'] * _secnd['marginradio'][1] * _secnd['pointvalue'])
            #============================
            if _state or abs(self.point-self.state.get(_account,{}).get('lastpoint',0))>=1:
                self.table.update({'InstrumentID': self.symbol, 'account': _account}, {
                    '$set': {'lastprice': self.point, 'marginall': _marginall, 'overdate': self.overdate,
                             'InstrumentID': self.symbol, '5result': self.state['result'], 'name': one['name'],
                             'account': _account, 'lastpoint': self.point, 'InstrumentName': self.name,
                             'master': self.master, 'show': self.master, 'from': 'core',
                             'bigtimer': time.time(), 'big': _big,
                             'update': datetime.datetime.now().strftime('%H%M%S')}}, upsert=True)
                self.state[_account]['lastpoint'] = self.point
                self.save_state()
                _id_list_begin = [x['id']*x['trend'] for x in self.state[_account]['grid'].values()]
                _id = self.state[_account].get('gridid',1)
                _dict = self.state[_account]['grid']
                _close_count = self.state[_account].get('closecount',0)

                rs = grid_loop(self.point,_dict,self.step,_grid_type,_close_count,last_trend_ = self.state[_account].get('lastlock',0),account_=_account,symbol_=self.symbol,id_ = _id,update_ = _update ,center_ = _center ,show_ = self.state[_account].get('isTrade',0)>0, plus_ = _trade_str)

                self.state[_account]['grid'] = rs['result']
                self.table.update({'InstrumentID':self.symbol,'account':_account},{'$inc':{'move':rs['close']-self.state[_account].get('closecount',0)}})
                self.state[_account]['closecount'] = rs['close']
                if rs.get('push',0)>0:
                    self.state[_account]['lastpoint'] = 0
                    self.save_state()
                if rs.get('loopopen',0)>0 and self.master > 0 and self.state[_account].get('isTrade', 0) > 0:
                    self.center['account'].update({'account':_account},{'$inc':{'grido':rs['loopopen']}})
                _id_list_end = [x['id']*x['trend'] for x in rs['result'].values()]
                if _id_list_end != _id_list_begin or rs['id']!=self.state[_account].get('gridid',1):
                    _all = list(self.table.find({'isTrade':1,'account':_account},{'_id':0,'InstrumentID':1,'move':1,'close':1,'lastid':1,'thisww':1}))
                    self.state[_account]['sumww'] = _sumww = sum([x.get('thisww',0.0) for x in _all])
                    self.state[_account]['sumid'] = _sumid = sum([x.get('move',0) for x in _all])
                    self.state[_account]['gridlevel'] = grid_level(self.state[_account]['grid'])
                    self.state[_account]['pk'] = _pk = 100.0/max(20.0,len(_all))
                    self.state[_account]['pklen'] = _pklen = len(_all)
                    self.state[_account]['gridprofit'] = rs['id']-len(rs['result'])+grid_profit(rs['result'],self.point)
                    self.state[_account]['floatprofit'] = grid_profit(rs['result'],self.point)
                    self.table.update({'InstrumentID':self.symbol,'account':_account},{'$inc':{'move':rs.get('push',0)},'$set':{'cnt':0,'close':rs['close'],'pk':_pk,'pklen':_pklen,'change':time.time()}})
                    if self.master > 0 and self.state[_account].get('isTrade', 0) > 0 and _account == DEMO_ID:
                        self.center['doubleMaster'].update_one({'symbol': self.symbol}, {
                            '$inc': {'open': rs.get('loopopen', 0), 'close': rs.get('loopclose', 0)}})
                    _month_version = '%s_%d' % ('.'.join(['%d_%d'%(x['id'],x['trend']) for x in self.state[_account]['grid'].values()]),self.state[_account].get('pklen', -1))
                    if self.state[_account].get('trade_status_version', '') != _month_version:
                        self.state[_account]['trade_status_version'] = _month_version
                        _all = list(self.table.find({'account': _account, 'master': 1}, {'_id': 0},
                                                    sort=[('big', desc), ('marginall', asc)]))
                        _sum = _margin
                        n = 0
                        _old = self.state[_account]['isTrade']
                        if _old == 0:
                            _level = -90
                        else:
                            _level = -85
                        self.state[_account]['isTrade'] = 0
                        _limit = money_limit(_sum / 10000) * 10000
                        while _sum > 0 and n < len(_all) and self.master > 0:
                            if 'marginall' in _all[n] and _all[n]['marginall'] <= _limit and _all[n].get('big', 0) > _level:
                                if _sum > _all[n]['marginall']:
                                    _sum -= _all[n]['marginall']
                                    if self.symbol == _all[n]['InstrumentID']:
                                        self.state[_account]['isTrade'] = 1
                            n += 1
                        if self.state[_account]['isTrade'] == 0 and self.master > 0:
                            pass
                self.state[_account]['gridid'] = rs['id']
                _plus = [x['trend']*abs(x['id']) for x in self.state[_account].get('grid',{}).values()]
                new_ = {}
                for k,v in self.state[_account].get('grid',{}).items():
                    v['lastprice'] = self.point
                    new_[k] = v
                self.state[_account]['grid'] = new_
                self.table.update({'InstrumentID':self.symbol,'account':_account},{'$set':{'updatedict':_update,'gridlevel':grid_level(self.state[_account].get('grid',{})),'gridid':self.state[_account].get('gridid',0),'grid':self.state[_account].get('grid',{}),\
                    'gridplatten':self.state[_account].get('grid',{}),'plus':_plus,'update':datetime.datetime.now().strftime('%H%M%S')}},upsert=True)
                if self.state[_account].get('gridid',-1)!=self.state[_account].get('lastid',-2):
                    self.state[_account]['lastid'] = self.state[_account].get('gridid',0)
                    if self.state[_account].get('thisww',0)==0:
                        self.state[_account]['thisww'] = self.state[_account]['lastid']*self.state[_account]['pfmr']
                    else:
                        self.state[_account]['thisww'] = self.state[_account]['thisww']+self.state[_account]['pfmr']/max(1,self.state[_account].get('gridlevel',0))
                    self.table.update({'InstrumentID':self.symbol,'account':_account},{'$set':{\
                        'isTrade':self.state[_account]['isTrade'],\
                        'trade_version':self.state[_account].get('trade_status_version',''),\
                        'lastid':self.state[_account].get('lastid',0),\
                        'close':self.state[_account].get('closecount',0),\
                        'float':self.state[_account].get('floatprofit',.0),\
                        'gridlevel':grid_level(self.state[_account].get('grid',{})),\
                        'thisww':self.state[_account].get('thisww',.0)}})
                self.save_state()
#===================================================================================
                _firstlot = 0
                _secndlot = 0
                if self.state[_account]['grid'] and self.master>0 and self.state[_account].get('isTrade',0)>0:
                    _dead = self.center['info'].find_one({'account': _account, 'key': 'deadline'}) or {}
                    if _dead.get('r',.0) < _dead.get('value',.0) or _dead.get('st',1.0)<.5:
                        _firstlot = sum([x.get('lot1',.0)*x['trend'] for x in filter(lambda x:x['id']>0,self.state[_account]['grid'].values())])
                        _secndlot = sum([x.get('lot2',.0)*x['trend'] for x in filter(lambda x:x['id']>0,self.state[_account]['grid'].values())])
                    else:
                        _firstlot = sum([x.get('lot1',.0)*x['trend'] for x in self.state[_account]['grid'].values()])
                        _secndlot = sum([x.get('lot2',.0)*x['trend'] for x in self.state[_account]['grid'].values()])
                self.conn_[_first['symbol']]['double'].update({'InstrumentID':self.symbol},{'$set':{'InstrumentID':self.symbol,'table.%s'%_account:   _firstlot,'from':'grid'}},upsert=True)
                self.conn_[_secnd['symbol']]['double'].update({'InstrumentID':self.symbol},{'$set':{'InstrumentID':self.symbol,'table.%s'%_account:-1*_secndlot,'from':'grid'}},upsert=True)
                self.save_state()
            for kpos in self.todo:
                self.cache[_show%kpos][0][_account] = {'grid':self.state[_account]['grid'],'lastid':self.state[_account].get('lastid',0),'gridprofit':self.state[_account].get('gridprofit',.0)}
                self.save_k(_show%kpos,self.cache[_show%kpos][0])
#        logger.error('%s run time %.3f [%.3f]'%(self.symbol,time.time()-self.tick_time,time.time()-self.trade_time))
    def save_state(self):
        self.state['a'] = self.first
        self.state['b'] = self.secend
        self.state['symbol'] = self.symbol
        self.state['time'] = time.time()
        self.state['product'] = self.product
        self.center['state'].update({'version':self.version},{'$set':self.state})
        return 0
        def save_h(a):
            self.center['state'].update({'version':self.version},{'$set':self.state})
        th_fork(save_h,(0,))
        return 0
    def trade_account(self,trade_accounts_):
        self.trade_accounts = trade_accounts_
        self.state['account_list'] = [str(x['account']) for x in trade_accounts_.values()]
        for one in self.trade_accounts.values():
            _acc = one['account']
            if _acc not in self.state:
                self.state[_acc] = {}
            self.state[_acc]['name'] = one['name']
    def get_image(self,group,lens,offset=0,see=None,account=None):
        if not see:
            result = list(self.savek.find({'group':self.aa%self.todo[-1]},sort=[('n',desc)],limit=int(lens),skip=int(offset)*int(lens)))
            if account and result:
                ot = []
                self.savek.delete_many({'group':self.aa%self.todo[-1],'n':{'$lt':result[0]['n']-500}})
                _lastgridlevel = 0
                _lastid = 0
                _lastprofit = 0
                for kk in result:
                    kk['tradingday'] = str(datetime.datetime.fromtimestamp(kk['time']-3600*3).isoweekday())
                    kk['grid'] = kk.get(account,{}).get('grid',{})
                    for k,v in kk['grid'].items():
                        kk['grid'][k]['open'] = self.point2price(v['open'])
                        kk['grid'][k]['stop'] = self.point2price(v['stop'])
                    if len(kk['grid'])==0:
                        kk['gridlevel'] = _lastgridlevel
                        kk['lastid'] = _lastid
                        kk['gridprofit'] = _lastprofit
                    else:
                        kk['gridlevel'] = _lastgridlevel = len(kk['grid'])
                        kk['lastid'] = _lastid = kk.get(account,{}).get('lastid',0)
                        kk['gridprofit'] = _lastprofit = kk.get(account,{}).get('gridprofit',.0)
                    kk['o'] = self.point2price(kk['o'])
                    kk['h'] = self.point2price(kk['h'])
                    kk['l'] = self.point2price(kk['l'])
                    kk['c'] = self.point2price(kk['c'])
                    ot.append(kk)
                result = ot
            out = SVG(group,result[::-1],[self.symbol,str(offset),'see']).to_html()
        else:
            result = list(self.savek.find({'group':self.aa%self.todo[see-1]},sort=[('n',desc)],limit=int(lens),skip=int(offset)*int(lens)))
            if account and result:
                ot = []
                self.savek.delete_many({'group':self.aa%self.todo[see-1],'n':{'$lt':result[0]['n']-500}})
                _lastgridlevel = 0
                _lastid = 0
                _lastprofit = 0
                for kk in result:
                    kk['tradingday'] = str(datetime.datetime.fromtimestamp(kk['time']-3600*3).isoweekday())
                    kk['grid'] = kk.get(account,{}).get('grid',{})
                    for k,v in kk['grid'].items():
                        kk['grid'][k]['open'] = self.point2price(v['open'])
                        kk['grid'][k]['stop'] = self.point2price(v['stop'])
                    if len(kk['grid'])==0:
                        kk['gridlevel'] = _lastgridlevel
                        kk['lastid'] = _lastid
                        kk['gridprofit'] = _lastprofit
                    else:
                        kk['gridlevel'] = _lastgridlevel = grid_level(kk['grid'])
                        kk['lastid'] = _lastid = kk.get(account,{}).get('lastid',0)
                        kk['gridprofit'] = _lastprofit = kk.get(account,{}).get('gridprofit',.0)
                    kk['o'] = self.point2price(kk['o'])
                    kk['h'] = self.point2price(kk['h'])
                    kk['l'] = self.point2price(kk['l'])
                    kk['c'] = self.point2price(kk['c'])
                    ot.append(kk)
                result = ot
            out = SVG(group,result[::-1],[self.symbol,str(offset),str(see)]).to_html()
        return out
    def load_state(self):
        state_ = self.center['state'].find_one({'version':self.version},{'_id':0})
        if not state_:
            state_ = {'long':1,'short':1,'ls':1,'result':{},'price':1.0,'lastprice':0.0,'point':0.0,'accounts':{},\
            'version':self.version,'symbol':self.symbol,'result':{},'InstrumentID':self.symbol,'change':0}
            self.center['state'].update({'version':self.version},{'$set':state_},upsert=True)
        self.state = state_
        _month_key = 'month_history'
        if _month_key not in self.state:
            self.state[_month_key] = {}
        if datetime.datetime.now().strftime('%Y_%m') not in self.state[_month_key]:
            self.state[_month_key][datetime.datetime.now().strftime('%Y_%m')] = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
            for _acc in self.state.get('account_list',[]):
                if _acc in self.state:
                    self.state[_acc]['closecount'] = _ww = int(self.state[_acc].get('closecount',0)*0.618)
                    _move = self.table.find_one({'InstrumentID':self.symbol,'account':_acc}) or {}
                    self.table.update({'InstrumentID':self.symbol,'account':_acc},{'$set':{'close':_ww,'move':int(.618*_move.get('move',0))}})
            self.save_state()
            if len(self.state[_month_key])>10:
                self.state[_month_key].pop(min(self.state[_month_key].keys()))
        if 'result' not in self.state:
            self.state['result'] = {}
        return self.state

#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
#================================================================================================================================================
class Carbon:
    def __init__(self,symbol,info,dbConnection,plus='',trade=False):
        self.conn_ = dbConnection
        self.trade = trade
        self.symbol = symbol
        self.demo_trade_id = int(time.time())*1000
        self.productid = ''
        self.logic = 'single'
        self.name = symbol
        self.pointvalue = 0.0
        self.marginradio = 0.01
        self.info = info
        self.master = -1
        self.step = 8.0
        self.maxstep = 8
        if self.info:
            self.pointvalue = self.info['VolumeMultiple']
            self.marginradio = max(self.info['ShortMarginRatio'],self.info['LongMarginRatio'])
            self.productid = self.info['ProductID']
            self.exchangeid = self.info['ExchangeID']
            self.pricetick = self.info['PriceTick']
            self.name = self.info['InstrumentName']
        self.plus = plus
        self.price = -1
        self.db = self.conn_[self.symbol]
        self.center = self.conn_[DB_NAME]
        self.raw = self.db['raw']
        self.vol = self.db['vol']
        self.tick = None
        self.tickhistory = self.db['tick']
        self.tickhistory.create_index([('time',desc)],background=True)
        self.double = self.db['double']
        self.table = self.db['table']
        self.UpdateTime = 0
        self.SkipTick = 0
        self.version = core_vsn + self.plus + self.logic + self.symbol
        self.todo = [3,5,7,9,11]
        self.todo_timeframes = {3:1,5:2,7:4,9:24,11:24}
        self.image = {'see':-1}
        for one in self.todo:
            self.db['a%d'%one].create_index([('n',desc),('do',desc)],background=True)
            self.db['b%d'%one].create_index([('n',desc),('do',desc)],background=True)
            self.db['k%d'%one].create_index([('n',desc),('do',desc)],background=True)
        self.timeshift = -5*3600
        self.hour = 0
        self.tradingday = '00000000'
        self.last_day = '00000000'
        self.last_volume = -1
        self.trade_accounts = {}
        self.cache = {}
        self.timer = 0
        self.table_timer = 0
        self.self_yesterday_hold = {}   #   自己维护昨仓  非上期所不区分今昨
        self.rtnAccountTradeO = {}
        self.rtnAccountTradeC = {}
    def get_demo_id(self):
        self.demo_trade_id += 1
        return self.demo_trade_id
    def setBridge(self,bridge):
        self.bridge = bridge
    def setMaster(self,tf):
        if tf:
            self.master = 1
        else:
            self.master = 0
        if self.last_volume < 0:
            self.last_volume = 0
            all_last = list(self.vol.find({},{'_id':0},sort=[('TradingDay',desc)],limit=5))
            _vd = {}
            for od in all_last:
                _vd[od['TradingDay']] = max(_vd.get(od['TradingDay'],0),od['Volume'])
            if len(_vd)>1:
                if '' in _vd:
                    _vd.pop('')
                _vd.pop(str(max(map(int,_vd.keys()))))
                _lastday = str(max(map(int,_vd.keys())))
                self.last_volume = sum(_vd.values())/len(_vd)
                self.last_day = _lastday
                self.center['symbols'].update({'InstrumentID':self.symbol},{'$set':{'_tick_':all_last[0],'_lastday':self.last_day,'_vol_':self.last_volume,'_allday_':_vd}})
            self.center['table'].update({'InstrumentID':self.symbol},{'$set':{'last_day':self.last_day,'last_volume':self.last_volume}},multi=True)
    def load_state(self):
        state_ = self.db['state'].find_one({'version':self.version},{'_id':0})
        if not state_:
            state_ = {'long':1,'short':1,'ls':1,'result':{},'price':1.0,'lastprice':0.0,'point':0.0,'accounts':{},\
            'version':self.version,'symbol':self.symbol,'result':{},'InstrumentID':self.symbol,'change':0}
            self.db['state'].update({'version':self.version},state_,upsert=True)
        self.state = state_
        if self.info:
            self.state['pointvalue'] = self.info['VolumeMultiple']
            self.state['marginradio'] = max(self.info['ShortMarginRatio'],self.info['LongMarginRatio'])
            self.state['ProductID'] = self.info['ProductID']
            self.state['InstrumentName'] = self.info['InstrumentName']
        if 'result' not in self.state:
            self.state['result'] = {}
        if len(self.state['result'])<len(self.todo):
            for i in self.todo:
                if str(i) not in self.state['result']:
                    self.state['result'][str(i)] = (0,-1)
        return self.state
    def eval_table(self):
        self.self_yesterday_hold = {}
        _all_account = self.center['table'].find({'InstrumentID':self.symbol})
        for one in list(_all_account):
            _acc = one['account']
            _sum = one.get('sum',0)
            if _acc in self.state and _sum != self.state[_acc].get('matchtable',0):
                self.state[_acc]['matchtable'] = _sum
                add_log(u'配平 %s %s %d'%(self.symbol,_acc,_sum),1200)
                self.save_state()
    def save_state(self):
        return self.db['state'].update({'version':self.version},{'$set':self.state})
    def trade_account(self,trade_accounts_):
        self.trade_accounts = trade_accounts_
        for one in self.trade_accounts.values():
            _acc = one['account']
            if _acc not in self.state:
                self.state[_acc] = {}
            self.state[_acc]['name'] = one['name']
# ======================================================================
# ======================================================================
# ======================================================================
# ======================================================================
# ======================================================================
    def in_position(self,_data,_account,clear_today_lock = False , clear_yd_lock = False,account_percent = .0):
        _data['sum'] = _data['todaylong' ]+_data['ydlong' ] - (_data['todayshort']+_data['ydshort'])
        if self.tradingday != '00000000' and self.exchangeid not in ['SHFE'] and self.productid in PRODUCT_FOR_LOCK:   #   上期所区分今仓昨仓！！！
            _key = 'selfyesterdayhold'
            if _account in self.self_yesterday_hold:
                _ydlong = self.self_yesterday_hold[_account].get('ydlong',0)
                _ydshrt = self.self_yesterday_hold[_account].get('ydshrt',0)
                if _data['todaylong'] >= _ydlong:
                    _data['todaylong'] -= _ydlong
                    _data['ydlong'] = _ydlong
                else:
                    self.self_yesterday_hold[_account]['ydlong'] = 0
                    self.db[_key].update({'account':_account},{'$set':{'ydlong':0}})
                    logger.error(u'%s %s %s 错误昨仓 收到多 %d 昨仓多 %d '%(self.exchangeid,self.symbol,_account,_data['todaylong'],_ydlong))
                if _data['todayshort'] >= _ydshrt:
                    _data['todayshort'] -= _ydshrt
                    _data['ydshort'] = _ydshrt
                else:
                    self.self_yesterday_hold[_account]['ydshrt'] = 0
                    self.db[_key].update({'account':_account},{'$set':{'ydshrt':0}})
                    logger.error(u'%s %s %s 错误昨仓 收到空 %d 昨仓空 %d '%(self.exchangeid,self.symbol,_account,_data['todayshort'],_ydshrt))
            else:
                _hold = self.db[_key].find_one({'account':_account}) or {}
                if _hold.get('tradingday','0') == self.tradingday:
                    self.self_yesterday_hold[_account] = _hold
                    add_log(u'#加载昨仓 %s %s 多:%d 空:%d'%(self.symbol,_account,_hold.get('ydlong',0),_hold.get('ydshrt',0)),1000)
                else:
                    self.self_yesterday_hold[_account] = _hold = {'ydlong':_data['todaylong'],'ydshrt':_data['todayshort'],'tradingday':self.tradingday,'account':_account}
                    self.db[_key].update({'account':_account},{'$set':_hold},upsert=True)
                    add_log(u'#设置昨仓 %s %s 多:%d 空:%d'%(self.symbol,_account,_hold.get('ydlong',0),_hold.get('ydshrt',0)),1000)

        if self.tick and time.time()-self.timer<4:
            _data['show'] = 1
            if clear_yd_lock and _data['ydlong']*_data['ydshort']>0:
                close_position(self, self.trade_accounts[_account], self.symbol, self.productid,self.exchangeid, self.tick,
                               1, _data['todaylong'], _data['ydlong'], 1,
                               self.pointvalue, self.marginradio, self.conn_)
                close_position(self, self.trade_accounts[_account], self.symbol, self.productid,self.exchangeid, self.tick,
                               1, _data['todayshort'], _data['ydshort'], -1,
                               self.pointvalue, self.marginradio, self.conn_)
                add_log(u'平昨仓锁单 %s %s'%(_account,self.symbol),1200)
            elif clear_today_lock and _data['todaylong']*_data['todayshort']>0:
                close_position(self, self.trade_accounts[_account], self.symbol, self.productid,self.exchangeid, self.tick,
                                     1, _data['todaylong'], _data['ydlong'], 1,
                                     self.pointvalue, self.marginradio, self.conn_)
                close_position(self, self.trade_accounts[_account], self.symbol, self.productid,self.exchangeid, self.tick,
                                     1, _data['todayshort'], _data['ydshort'], -1,
                                     self.pointvalue, self.marginradio, self.conn_)
                add_log(u'平今仓锁单 %s %s' % (_account, self.symbol), 1200)
            if self.trade_accounts[_account]['trade']<0:
                if _data['todaylong' ]+_data['ydlong' ]>0:
                    close_position(self,self.trade_accounts[_account],self.symbol,self.productid,self.exchangeid,self.tick,_data['todaylong' ]+_data['ydlong' ],_data['todaylong' ],_data['ydlong' ],1,self.pointvalue,self.marginradio,self.conn_)
                if _data['todayshort']+_data['ydshort']>0:
                    close_position(self,self.trade_accounts[_account],self.symbol,self.productid,self.exchangeid,self.tick,_data['todayshort']+_data['ydshort'],_data['todayshort'],_data['ydshort'],-1,self.pointvalue,self.marginradio,self.conn_)
        self.state[_account].update(_data)
        self.state[_account]['percent'] = account_percent
        self.center['table'].update({'InstrumentID':self.symbol,'account':_account},{'$set':_data},upsert=True)
    def in_order(self,_data,_account):
        _key = {'InstrumentID':self.symbol,'account':_account,'OrderRef':_data['OrderRef'],'client_version':_data['client_version']}
        _dict = {'time':time.time()}
        for kk in ['StatusMsg','VolumeTraded']:
            _dict[kk] = _data[kk]
        self.center['SignalHistory'].update(_key,{'$set':{'orderstatus.%s'%_data['OrderStatus']:_dict}})
        self.center['error'].update(_key,{'$set':{'orderstatus.%s'%_data['OrderStatus']:_dict}})
        logger.error(str(('order',self.symbol,_account,_data['OrderStatus'],_dict)))
    def in_trade(self,_data,_account):
        _key = {'InstrumentID':self.symbol,'account':_account,'OrderRef':_data['OrderRef'],'client_version':_data['client_version']}
        _dict = {'time':time.time()}
        for kk in ['Price','Volume']:
            _dict[kk] = _data[kk]
        self.center['SignalHistory'].update(_key,{'$set':{'at':self.state.get('at',0.0),'aw':self.state.get('aw',0.0),'traded':1,'tradeid.%s'%_data['TradeID']:_dict}})
        self.center['error'].update(_key,{'$set':{'tradeid.%s'%_data['TradeID']:_dict}})
        if int(_data['OrderRef']) in self.rtnAccountTradeO.get(_account,{}):
            self.rtnAccountTradeO[_account][int(_data['OrderRef'])][_data['TradeID']] = _dict
            _list = self.rtnAccountTradeO[_account][int(_data['OrderRef'])].values()
            _sum = sum([x['Volume'] for x in _list])
            _psum = sum([x['Volume']*x['Price'] for x in _list])
            _price = _psum/_sum
            self.center['table'].update({'InstrumentID':self.symbol,'account':_account},{'$set':{'lastopenprice':_price,'lastopenvolume':_sum,'lastopentime':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
        if int(_data['OrderRef']) in self.rtnAccountTradeC.get(_account,{}):
            self.rtnAccountTradeC[_account][int(_data['OrderRef'])][_data['TradeID']] = _dict
            _list = self.rtnAccountTradeC[_account][int(_data['OrderRef'])].values()
            _sum = sum([x['Volume'] for x in _list])
            _psum = sum([x['Volume']*x['Price'] for x in _list])
            _price = _psum/_sum
            self.center['table'].update({'InstrumentID':self.symbol,'account':_account},{'$set':{'lastcloseprice':_price,'lastclosevolume':_sum,'lastclosetime':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
        logger.error(str(('trade',self.symbol,_account,_data['TradeID'],_dict)))
    # ======================================================================
    def trade_skip(self):
        for one in self.trade_accounts.values():
            _acc = one['account']
            h = self.center['table'].find_one({'InstrumentID':self.symbol,'account':_acc}) or {}
            if h.get('onoff',0)!=0:
                self.center['table'].update({'InstrumentID':self.symbol,'account':_acc},{'$set':{'onoff':0}},upsert=True)
                add_log(u'<font color="red">%s 触发涨跌停板规避</font>'%self.symbol,3600)
    def get_todo(self):
        return self.todo
    def new_price(self,timer,tick):
        if timer - self.table_timer > 1800:
            self.table_timer = timer
            self.eval_table()
        self.timer = timer
        self.tick = tick
        self.vol.update({'TradingDay':tick['TradingDay']},{'$set':tick},upsert=True)
        if self.tick['TradingDay'] and int(self.tick['TradingDay'])>int(self.tradingday):
            self.tradingday = tick['TradingDay']
        self.price = self.tick['LastPrice']
        self.point = self.price2point(self.price)
        self.table.update({'InstrumentID':self.symbol},{'$set':{'lastprice':self.price,'PointValue':self.state['pointvalue'],'MarginRadio':self.state['marginradio']}},multi=True)
    def price2point(self,price):
        return math.log(price)*Point_Multi
    def do_price(self,name_,point_,len_):
        self.pos = len_
        _result = list(self.db[name_].find({'do':1},{'_id':0},sort=[('n',desc)],limit=2))
        if len(_result)>0:
            now = _result[0]
            if len(_result)>1:
                last = _result[1]
            else:
                last = {}

            now['c'] = point_
            now['h'] = max(now['c'],now['h'])
            now['l'] = min(now['c'],now['l'])
            now['pos'] = len_
            now['do'] = 0
            now,last = self.check_k_len(now,last,name_,len_)
            now,last = self.check_k_period(now,last,name_,self.todo_timeframes[len_]*3600)
            self.check_base(name_,now,last)
        else:
            if name_[0]=='b':
                _result = list(self.db['k1'].find({'do':1},{'_id':0},sort=[('cnt',desc)],limit=2))
            else:
                _result = list(self.db['k8'].find({'do':1},{'_id':0},sort=[('cnt',desc)],limit=2))
            if len(_result)>0:
                now = _result[0]
                if len(_result)>1:
                    last = _result[1]
                    if last.get('n',0)>100:
                        self.db[name_].delete_many({'n':{'$lt':last['n']-100}})
                else:
                    last = {}

                now['c'] = point_
                now['h'] = max(now['c'],now['h'])
                now['l'] = min(now['c'],now['l'])
                now['pos'] = len_
                now['do'] = 0
                now,last = self.check_k_len(now,last,name_,len_)
                now,last = self.check_k_period(now,last,name_,self.todo_timeframes[len_]*3600)
                self.check_base(name_,now,last)
            else:
                last = {}
                now = {'do':0,'o':point_,'h':point_,'l':point_,'c':point_,'hour':0,'point':point_,'n':0}
                self.check_base(name_,now,last)
    def check_base(self,_name,_todo,_last):
        _result = True
        if _name[0] != 'k':
            _result = False
            _todo,_last = fill_base(_todo,_last,_flow='ABC')
        _todo['do'] = 1
        _todo['pos'] = self.pos
        _todo['ll'] = _todo['h']-_todo['l']
        _todo['time'] = time.time()
        _todo['tradingday'] = self.tradingday
        if _last:
            _todo['n'] = _last.get('n',0)+1
            _todo['point'] = self.state.get(str(_todo['pos']),{}).get('point',0)#_last.get('point',0)
            _todo['result'] = self.state.get(str(_todo['pos']),{}).get('ls',0)#_last.get('result',0)
            self.cache[_name] = [_todo,_last]
            self.save_k(_name,_last)
            self.save_k(_name,_todo)
            if _result and _todo['n']>=1:               #=============================================================================================================
                self.get_result(name=_name)
        else:
            self.cache[_name] = [_todo]
            self.save_k(_name,_todo)
        return _todo
    def check_k_period(self,now,last,name,timeframe):
        _hour = int(self.timer+self.timeshift)/timeframe
        if now.get('hour',0)!=_hour:# or (now.get('_doit',0)>0 and self.value>0):
#        if now.get('hour',0)!=_hour and last.get('n',0)<=50:# or (now.get('_doit',0)>0 and self.value>0):
            p = now['c']
            new = {'o':p,'h':p,'l':p,'c':p,'do':0,'hour':_hour,'point':now.get('point',0)}
            now = self.check_base(name,now,last)
            return (new,now)
        return (now,last)
    def check_k_len(self,now,last,name,len_,n=0):
        if name[0]=='b':
            length = self.state.get('w%d'%len_,0)/self.bridge or 1
#        elif len_ == self.todo[0]:
#            length = self.step = now['len'] = max(1.0,self.state.get('w%d'%len_,0)/10)
        else:
            length = now['len'] = len_
        if now['h']-now['o']>length and n<100:
            high = now['h']
            now['h'] = now['o']+length
            now['c'] = now['o']+length
            new = {'o':now['c'],'h':high,'l':now['c'],'c':now['c'],'do':0,'hour':now['hour'],'point':now.get('point',0)}
            now = self.check_base(name,now,last)
            return self.check_k_len(new,now,name,len_,n=n+1)
        elif now['o']-now['l']>length and n<100:
            low = now['l']
            now['l'] = now['o']-length
            now['c'] = now['o']-length
            new = {'o':now['c'],'h':now['c'],'l':low,'c':now['c'],'do':0,'hour':now['hour'],'point':now.get('point',0)}
            now = self.check_base(name,now,last)
            return self.check_k_len(new,now,name,len_,n=n+1)
        elif name[0]=='k' and now.get('n',0)==self.state.get(str(len_),{}).get('tradepos',0) and n<100:
            p = now['c']
            new = {'o':p,'h':p,'l':p,'c':p,'do':0,'hour':now['hour'],'point':now.get('point',0)}
            now = self.check_base(name,now,last)
            return self.check_k_len(new,now,name,len_,n=n+1)
        else:
            return (now,last)
    def save_k(self,name_,data_):
        _key = {'n':data_['n']}
        self.db[name_].update(_key,{'$set':data_},upsert=True)
    def get_image(self,group,lens,offset=0,see=None,account=None):
        if not see:
            result = list(self.db['a%d'%self.todo[self.image['see']]].find(sort=[('n',desc)],limit=int(lens),skip=int(offset)*int(lens)))
            if account:
                ot = []
                for kk in result:
                    kk['grid'] = kk.get(account,{}).get('grid',{})
                    ot.append(kk)
                result = ot
            out = SVG(group,result[::-1],[self.symbol,str(offset),'see']).to_html()
        else:
            result = list(self.db['a%d'%self.todo[see-1]].find(sort=[('n',desc)],limit=int(lens),skip=int(offset)*int(lens)))
            if account:
                ot = []
                for kk in result:
                    kk['grid'] = kk.get(account,{}).get('grid',{})
                    ot.append(kk)
                result = ot
            out = SVG(group,result[::-1],[self.symbol,str(offset),str(see)]).to_html()
        return out
    def get_image_big(self,group,lens,offset=0,see=None):
        if not see:
            result = list(self.db['a%d'%self.todo[self.image['see']]].find(sort=[('n',desc)],limit=int(lens),skip=int(offset)*int(lens)))
            out = SVG(group,result[::-1],[self.symbol,str(offset),'see']).to_html()
        else:
            result = list(self.db['a%d'%self.todo[see-1]].find(sort=[('n',desc)],limit=int(lens),skip=int(offset)*int(lens)))
            out = SVG(group,result[::-1],[self.symbol,str(offset),str(see)]).to_html()
        return out
    #=====================================================================
    def get_power(self):
        return self.state.get('power',0.0)
    def plus_table(self,_acc,_table):
        if _acc not in self.state:
            self.state[_acc] = {}
        self.state[_acc]['table'] = _table
        self.save_state()
    def get_result(self,name=''):
        c = self.cache
        def just(uu,nn,u,n):
            a = max(0,uu-u)
            b = max(0,n-nn)
            a = uu-u
            b = n-nn
            return -100*(a-b)/max(1,a+b)
        pos = c[name][0]['pos']
        bb = 'b%d'%pos
        aa = 'a%d'%pos
        kk = 'k%d'%pos

        s = self.state.get(str(pos),{'long':0,'short':0,'ls':0,'price':self.price,'point':self.point})

        k1u = max([border(c[bb][0], 1,x,'A',p=1) for x in [3,4,5,6,7]])
        k1n = min([border(c[bb][0],-1,x,'A',p=1) for x in [3,4,5,6,7]])
        k1up = max([border(c[bb][0], 1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k1np = min([border(c[bb][0],-1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k8u = max([border(c[aa][0], 1,x,'A',p=-1) for x in [3,4,5,6,7]])
        k8n = min([border(c[aa][0],-1,x,'A',p=-1) for x in [3,4,5,6,7]])
        k8up = max([border(c[aa][0], 1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k8np = min([border(c[aa][0],-1,x,'C',p=-1) for x in [3,4,5,6,7]])
        k8o = border(c[aa][0],0,8,'A',p=0)

        slevel = sun = just(k8o+self.todo_timeframes[pos]*(1-myth)*10,k8o-self.todo_timeframes[pos]*(1-myth)*10,k1u,k1n)
        kkk = abs(slevel)
        _kun = k1up+k1np

        aaa = border(c[aa][0], 1,8,'A',p=-1)-border(c[aa][0], 1,7,'A',p=-1)
        bbb = border(c[aa][0],-1,8,'A',p=-1)-border(c[aa][0],-1,7,'A',p=-1)
        if pos==self.todo[-1]:
            self.state['power'] = (aaa-bbb)/(aaa+bbb)

        if k8up>100:
            wu = 100-(k8up-100)
        else:
            wu = 100+(100-k8up)
        if k8np<-100:
            wn = -100+(-100-k8np)
        else:
            wn = -100-(k8np+100)

        bu = k1np+100+(k1up-100)*myth
        bn = k1up-100+(k1np+100)*myth

        self.state['center'] = k8o
        self.state['sun'] = slevel
        self.save_state()

        self.state['w%d'%pos] = k8u-k8n
        s['Alevel'] = slevel
        s['bu'] = bu
        s['bn'] = bn
        s['wu'] = wu
        s['wn'] = wn

        for one in [kk,aa]:
            c[one][0]['kun'] = _kun
            c[one][0]['sun'] = slevel
            c[one][0]['bu'] = bu
            c[one][0]['bn'] = bn
            c[one][0]['k1up'] = k1up
            c[one][0]['k1np'] = k1np
            c[one][0]['k8up'] = k8up
            c[one][0]['k8np'] = k8np
            c[one][0]['k8ua'] = k8u
            c[one][0]['k8na'] = k8n
            c[one][0]['wu'] = wu
            c[one][0]['wn'] = wn
            self.save_k(one,c[one][0])


        Short = s['short']
        Long = s['long']
        LS = s['ls']
        Price = s['price']
        Point = s['point']

        if Short==0 and s.get('tradepos',0) != c[kk][0]['n']:
            if 1:
                if Long<=0 and bu>wu:
                    s['long'] = Long = 1
                    if self.master>0:
#                        add_log(u'<font color="red">%s 多头确认 %d</font>'%(self.symbol,pos),0)
                        s['change'] = time.time()
                if Long>=0 and bn<wn:
                    s['long'] = Long = -1
                    if self.master>0:
#                        add_log(u'<font color="green">%s 空头确认 %d</font>'%(self.symbol,pos),0)
                        s['change'] = time.time()
            if 1:# DON'T CHANGE HERE
                if Short==0 and bu>wu and k1np>0:
                    s['long'] = Long = 1
                    s['short'] = Short = 1
                    s['touch'] = time.time()
                if Short==0 and bn<wn and k1up<0:
                    s['short'] = Short = -1
                    s['long'] = Long = -1
                    s['touch'] = time.time()
        elif s.get('tradepos',0) != c[kk][0]['n']:
            if 1:
                if Long<=0 and bu>wu:
                    s['long'] = Long = 1
                    s['short_pos'] = c[kk][0]['c']
                    if self.master>0:
#                        add_log(u'<font color="red">%s 多头确认 %d</font>'%(self.symbol,pos),0)
                        s['change'] = time.time()
                if Long>=0 and bn<wn:
                    s['long'] = Long = -1
                    s['short_pos'] = c[kk][0]['c']
                    if self.master>0:
#                        add_log(u'<font color="green">%s 空头确认 %d</font>'%(self.symbol,pos),0)
                        s['change'] = time.time()
            if Short>0 and bu<wu and k1np<0:
                s['short'] = Short = 0
                s['touch'] = time.time()
            if Short<0 and bn>wn and k1up>0:
                s['short'] = Short = 0
                s['touch'] = time.time()

        LS2 = Short

        if type(self.state['result']) != type({}):
            self.state['result'] = {}
            self.save_state()

        if LS2!=LS:
            self.SkipTick = time.time()+Wait_Result
            s['ls'] = LS2
            _profit = LS*(self.price-Price)
            s['tradeid'] = s.get('tradeid',0)+1
            _dict = {'tradeid':s['tradeid'],'InstrumentID':self.symbol,'logic':self.logic,'n':c[kk][0]['n'],'otime':s.get('tradetime','- -'),'ctime':time2datetime(self.timer).strftime('%Y%m%d %H%M%S'),'oprice':s['price'],'cprice':self.price,'lot':1,'signal':LS,'pos':pos}
            _dict['date'] = datetime.datetime.now().strftime('%Y%m%d')
            _dict['kk'] = 100*(_dict['cprice']-_dict['oprice'])/max(1,_dict['oprice'])*_dict['signal']
            _dict['mr'] = self.marginradio
            _dict['mk'] = _dict['kk']/_dict['mr']
            if self.master>0 and self.productid in Inst_List:
                self.center['LogicHistory'].update({'tradeid':s['tradeid'],'InstrumentID':self.symbol,'logic':self.logic},{'$set':_dict},upsert=True)
                Demo_Fee(self.symbol,self.productid,self.exchangeid,self.price,self.pointvalue,self.marginradio,get_lot(Demo_Update()['Balance'],self.price,self.pointvalue,self.marginradio),pos,c[kk][0]['n'],self.timer)
            s['price'] = self.price
            s['tradetime'] = time2datetime(self.timer).strftime('%Y%m%d %H%M%S')
            s['profit'] = _profit
            s['point'] = Point = c[kk][0]['c']
            s['tradepos'] = c[kk][0]['n']
            s['result'] = LS2
            c[kk][0]['point'] = Point
            c[kk][0]['result'] = LS2
            self.save_k(kk,c[kk][0])
            c[aa][0]['point'] = Point
            c[aa][0]['result'] = LS2
            self.save_k(aa,c[aa][0])
            if self.master > 0:
                self.state['result'][str(pos)] = (Short,s.get('touch',0))
            self.state[str(pos)] = s
            self.save_state()
        else:
            c[kk][0]['point'] = Point
            c[kk][0]['result'] = LS
            self.save_k(kk,c[kk][0])
    def set_match_table(self,_account,_group,_table):
        if not self.tick:return 0
        if _account not in self.state:
            self.state[_account] = {}
        if 'match' not in self.state[_account]:
            self.state[_account]['match'] = {}
        self.state[_account]['match'][_group] = _table
        table_ = sum(self.state[_account]['match'].values())
        old_table = self.state[_account].get('matchtable',0)
        _percent = self.state[_account].get('percent',.0)
        if old_table != table_ and _account in self.trade_accounts:
            db_key = 'selfyesterdayhold'
            self.state[_account]['matchtable'] = table_
            self.center['table'].update({'InstrumentID': self.symbol, 'account': _account}, {'$set': {'matchtable':table_}}, upsert=True)
            self.state[_account]['change'] = time.time()
            if old_table*table_>0:
                if abs(table_)>abs(old_table):  #   open
                    _lot = abs(table_)-abs(old_table)
                    _trade = self.trade_accounts[_account]
                    _inst = self.symbol
                    _tick = self.tick
                    if table_>0:
                        _trend = 1
                        reverse_haved = self.state[_account].get('ydshort',0)
                        if reverse_haved >= _lot:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todayshort', 0),
                                           self.state[_account].get('ydshort', 0), -1*_trend, self.pointvalue,
                                           self.marginradio, self.conn_)

                            if _account in self.self_yesterday_hold:
                                _key = 'ydshrt'
                                self.self_yesterday_hold[_account][_key] = self.self_yesterday_hold[_account][_key] - _lot
                                self.db[db_key].update({'account':_account},{'$inc':{_key:-1*_lot}})

                            add_log(u'%s %s %s 开多 %d [平空]'%(_group,_account,_inst,_lot),1000)
                        else:
                            open_position(self,_trade,_inst,self.exchangeid,_tick,_lot,_trend,self.conn_,self.pointvalue,self.marginradio,self.productid)
                            add_log(u'%s %s %s 开多 %d'%(_group,_account,_inst,_lot),1000)
                    else:
                        _trend = -1
                        reverse_haved = self.state[_account].get('ydlong',0)
                        if reverse_haved >= _lot:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todaylong', 0),
                                           self.state[_account].get('ydlong', 0), -1*_trend, self.pointvalue,
                                           self.marginradio, self.conn_)

                            if _account in self.self_yesterday_hold:
                                _key = 'ydlong'
                                self.self_yesterday_hold[_account][_key] = self.self_yesterday_hold[_account][_key] - _lot
                                self.db[db_key].update({'account':_account},{'$inc':{_key:-1*_lot}})

                            add_log(u'%s %s %s 开空 %d [平多]'%(_group,_account,_inst,_lot),1000)
                        else:
                            open_position(self,_trade,_inst,self.exchangeid,_tick,_lot,_trend,self.conn_,self.pointvalue,self.marginradio,self.productid)
                            add_log(u'%s %s %s 开空 %d'%(_group,_account,_inst,_lot),1000)
                else:                           #   close
                    _lot = abs(old_table)-abs(table_)
                    _trade = self.trade_accounts[_account]
                    _inst = self.symbol
                    _tick = self.tick
                    if table_>0:
                        _trend = 1
                        _haved = self.state[_account].get('ydlong', 0)
                        if _haved <= _lot and self.productid in PRODUCT_FOR_LOCK and _percent < TRADE_LOCK_TODAY + TRADE_LOCK_RANGE:
                            open_position(self,_trade,_inst,self.exchangeid,_tick,_lot,-1*_trend,self.conn_,self.pointvalue,self.marginradio,self.productid)
                            add_log(u'%s %s %s 平多 %d [开空 %.1f]' % (_group, _account, _inst, _lot,_percent), 1000)
                        else:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todaylong',0), self.state[_account].get('ydlong',0),_trend,self.pointvalue,self.marginradio,self.conn_)
                            add_log(u'%s %s %s 平多 %d'%(_group,_account,_inst,_lot),1000)
                    else:
                        _trend = -1
                        _haved = self.state[_account].get('ydshort', 0)
                        if _haved <= _lot and self.productid in PRODUCT_FOR_LOCK and _percent < TRADE_LOCK_TODAY + TRADE_LOCK_RANGE:
                            open_position(self,_trade,_inst,self.exchangeid,_tick,_lot,-1*_trend,self.conn_,self.pointvalue,self.marginradio,self.productid)
                            add_log(u'%s %s %s 平空 %d [开多 %.1f]' % (_group, _account, _inst, _lot,_percent), 1000)
                        else:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todayshort',0), self.state[_account].get('ydshort',0),_trend,self.pointvalue,self.marginradio,self.conn_)
                            add_log(u'%s %s %s 平空 %d'%(_group,_account,_inst,_lot),1000)
            else:
                #close old_table
                if abs(old_table)>0:            #   close
                    _trade = self.trade_accounts[_account]
                    _inst = self.symbol
                    _tick = self.tick
                    _lot = abs(old_table)
                    if old_table>0:
                        _trend = 1
                        _haved = self.state[_account].get('ydlong', 0)
                        if _haved <= _lot and self.productid in PRODUCT_FOR_LOCK and _percent < TRADE_LOCK_TODAY + TRADE_LOCK_RANGE:
                            open_position(self, _trade, _inst, self.exchangeid, _tick, _lot, -1 * _trend, self.conn_,
                                          self.pointvalue, self.marginradio, self.productid)
                            add_log(u'%s %s %s 平多 %d [开空 %.1f]' % (_group, _account, _inst, _lot,_percent), 1000)
                        else:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todaylong', 0),
                                           self.state[_account].get('ydlong', 0), _trend, self.pointvalue,
                                           self.marginradio, self.conn_)
                            add_log(u'%s %s %s 平多 %d' % (_group, _account, _inst, _lot), 1000)
                    else:
                        _trend = -1
                        _haved = self.state[_account].get('ydshort', 0)
                        if _haved <= _lot and self.productid in PRODUCT_FOR_LOCK and _percent < TRADE_LOCK_TODAY + TRADE_LOCK_RANGE:
                            open_position(self, _trade, _inst, self.exchangeid, _tick, _lot, -1 * _trend, self.conn_,
                                          self.pointvalue, self.marginradio, self.productid)
                            add_log(u'%s %s %s 平空 %d [开多 %.1f]' % (_group, _account, _inst, _lot,_percent), 1000)
                        else:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todayshort', 0),
                                           self.state[_account].get('ydshort', 0), _trend, self.pointvalue,
                                           self.marginradio, self.conn_)
                            add_log(u'%s %s %s 平空 %d' % (_group, _account, _inst, _lot), 1000)
                if abs(table_)>0:               #   open
                    _lot = abs(table_)
                    _trade = self.trade_accounts[_account]
                    _inst = self.symbol
                    _tick = self.tick
                    if table_>0:
                        _trend = 1
                        reverse_haved = self.state[_account].get('ydshort',0)
                        if reverse_haved >= _lot:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todayshort', 0),
                                           self.state[_account].get('ydshort', 0), -1 * _trend, self.pointvalue,
                                           self.marginradio, self.conn_)

                            if _account in self.self_yesterday_hold:
                                _key = 'ydshrt'
                                self.self_yesterday_hold[_account][_key] = self.self_yesterday_hold[_account][_key] - _lot
                                self.db[db_key].update({'account':_account},{'$inc':{_key:-1*_lot}})

                            add_log(u'%s %s %s 开多 %d [平空]' % (_group, _account, _inst, _lot), 1000)
                        else:
                            open_position(self, _trade, _inst, self.exchangeid, _tick, _lot, _trend, self.conn_,
                                          self.pointvalue, self.marginradio, self.productid)
                            add_log(u'%s %s %s 开多 %d' % (_group, _account, _inst, _lot), 1000)
                    else:
                        _trend = -1
                        reverse_haved = self.state[_account].get('ydlong',0)
                        if reverse_haved >= _lot:
                            close_position(self, _trade, _inst, self.productid, self.exchangeid, _tick, _lot,
                                           self.state[_account].get('todaylong', 0),
                                           self.state[_account].get('ydlong', 0), -1 * _trend, self.pointvalue,
                                           self.marginradio, self.conn_)

                            if _account in self.self_yesterday_hold:
                                _key = 'ydlong'
                                self.self_yesterday_hold[_account][_key] = self.self_yesterday_hold[_account][_key] - _lot
                                self.db[db_key].update({'account':_account},{'$inc':{_key:-1*_lot}})

                            add_log(u'%s %s %s 开空 %d [平多]' % (_group, _account, _inst, _lot), 1000)
                        else:
                            open_position(self, _trade, _inst, self.exchangeid, _tick, _lot, _trend, self.conn_,
                                          self.pointvalue, self.marginradio, self.productid)
                            add_log(u'%s %s %s 开空 %d' % (_group, _account, _inst, _lot), 1000)
        self.save_state()
    def get_trade(self):
        _table = list(self.double.find())
        _sum = {}
        for _line in _table:
            for _acc,_lot in _line.get('table',{}).items():
                if _acc not in _sum:
                    _sum[_acc] = 0.0
                _sum[_acc] += _lot
        for one in self.trade_accounts.values():
            _account = one['account']
            self.center['table'].update({'InstrumentID':self.symbol,'account':_account},{'$set':{'InstrumentID':self.symbol,'name':one['name'],'account':_account,'lastprice':self.price,'InstrumentName':self.name,'master':self.master,'show':self.master,'from':'core'}},upsert=True)
            if _account in _sum:
                if _lot>0:
                    _lot = int(_sum[_account]+.5)
                else:
                    _lot = int(_sum[_account]-.5)
                if one['trade']>0:
                    if _account == DEMO_ID:
                        old_table = self.state[_account].get('table',0)
                        if old_table != _lot:
                            self.state[_account]['table'] = _lot
                            self.save_state()
                            if old_table*_lot>=0:
                                if abs(_lot)>abs(old_table):
                                    # open
                                    _lot_ = abs(_lot)-abs(old_table)
                                    if _lot>0:
                                        _trend_ = 1
                                    else:
                                        _trend_ = -1
                                    Demo_Open(self.symbol,self.productid,self.exchangeid,self.price,self.pointvalue,self.marginradio,_lot_,_trend_,self.get_demo_id())
                                else:
                                    # close
                                    _lot_ = abs(old_table)-abs(_lot)
                                    if old_table>0:
                                        _trend_ = 1
                                    else:
                                        _trend_ = -1
                                    Demo_Close(self.symbol,self.productid,self.exchangeid,self.price,self.pointvalue,self.marginradio,_lot_,_trend_,self.get_demo_id())
                            else:
                                # close old_table
                                if abs(old_table)!=0:
                                    if _lot>0:
                                        _trend_ = -1
                                    else:
                                        _trend_ = 1
                                    Demo_Close(self.symbol,self.productid,self.exchangeid,self.price,self.pointvalue,self.marginradio,abs(old_table),_trend_,self.get_demo_id())
                                if abs(_lot)!=0:
                                    # open _lot
                                    if _lot>0:
                                        _trend_ = 1
                                    else:
                                        _trend_ = -1
                                    Demo_Open(self.symbol,self.productid,self.exchangeid,self.price,self.pointvalue,self.marginradio,abs(_lot),_trend_,self.get_demo_id())
                    else:
                        # mqtt_table
                        self.set_match_table(_account,'self',_lot)
#                        self.bridge.publish_table({'symbol':self.symbol,'account':_account,'table':_lot,'group':'double'})
                elif one['trade']<0:
                    if 1:
                        if _account == DEMO_ID:
                            pass
                        else:
                            #   mqtt_table 0
                            self.set_match_table(_account, 'self', 0)
#                            self.bridge.publish_table({'symbol':self.symbol,'account':_account,'table':0,'group':'double'})
        return 0
