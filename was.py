from twisted.web import server, resource, static
from twisted.internet import reactor, endpoints
import json
import sqlite3
import itertools
import operator
import datetime
import uuid

conn = sqlite3.connect('db.db')

def validateDateFormat(date_text):
    if(len(date_text)<8):
        return False

    try:
        datetime.datetime.strptime(date_text, '%Y%m%d')
    except ValueError:
        return False
    
    return True

def most_common(L):
  # get an iterable of (item, iterable) pairs
  SL = sorted((x, i) for i, x in enumerate(L))
  # print 'SL:', SL
  groups = itertools.groupby(SL, key=operator.itemgetter(0))
  # auxiliary function to get "quality" for an item
  def _auxfun(g):
    item, iterable = g
    count = 0
    min_index = len(L)
    for _, where in iterable:
      count += 1
      min_index = min(min_index, where)
    # print 'item %r, count %r, minind %r' % (item, count, min_index)
    return count, -min_index
  # pick the highest-count/earliest item
  return max(groups, key=_auxfun)[0]

def element_removal(L,rem_keyword):
    L2 = []
    for x in range(0,len(L)):
        if L[x] != rem_keyword:
            L2.append(L[x])
    return L2

def findTeam(keyword):
    #sql_str = "select name,code from name_collection_eng where 1=1 "
    sql_str = "select code from name_collection_eng where 1=1 "
    c = conn.cursor()

    returnValue = keyword
    found_results = []
    sp = 0
    first_letter = True
    for x in range(1,len(keyword)+1):
        #print keyword[sp:x]
        if keyword[sp:x] == ' ':
            sp+=1
            continue

        if first_letter :
            #sql_str_t = "select name,code from ( select * from name_collection_eng where name like '" + keyword[sp:x] + "%' union all select * from name_collection_eng where name like '%" + keyword[sp:x] + "%' and name not like '" + keyword[sp:x] + "%' )" 
            sql_str_t = "select code from ( select * from name_collection_eng where name like '" + keyword[sp:x] + "%' union all select * from name_collection_eng where name like '%" + keyword[sp:x] + "%' and name not like '" + keyword[sp:x] + "%' )" 
            first_letter=False
        else:
            sql_str_t = sql_str + " and name like '%" + keyword[sp:x] + "%'"

        c.execute(sql_str_t);
        results = c.fetchall()
        for result in results:
            #found_results.append(result[0]+":"+str(result[1]))
            found_results.append(str(result[0]))
        sp+=1

    if len(found_results) > 0:
        returnValue = most_common(found_results)
    
    return returnValue

class test(resource.Resource):
    isLeaf = True
    numberRequests = 0

    def render_GET(self, request):
        self.numberRequests += 1
        request.setHeader("content-type", "text/plain")
        return "I am request #" + str(self.numberRequests) + "\n"

class searchEngine(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        request.setHeader("content-type", "application/json")
        #request.setHeader('Access-Control-Allow-Origin', '*')
        #print('==============')
        #callback = request.args['callback'][0] 
        keyword = request.args['keyword'][0]
        keyword = keyword.decode('utf-8')
        #jsonData = json.loads(request.content.getvalue())
        #keyword = jsonData['keyword']

        if(keyword==''):
            #return callback+"()"
            return ""

        teamA = ''
        teamB = ''
        dateTime = ''
        found_teamB = False

        checkComma = keyword.find(',')                            #Check (,)
        if(keyword.find(',') != -1):                              #if exist,
            #print "teamA: " + keyword[:keyword.find(',')]
            teamA = findTeam(keyword[:keyword.find(',')])         #find teamA
            keyword = keyword[keyword.find(','):]                 #get remains without teamA
            checkComma = keyword.find(',')                        #Check (,)
            if(checkComma != -1 and len(keyword) > 1):            #if exist with some words. 
                keyword = keyword[1:]
                if(keyword.find(',') != -1):
                    #print "teamB: " + keyword[:keyword.find(',')]
                    teamB = findTeam(keyword[:keyword.find(',')]) #find teamB
                    found_teamB = True
                    keyword = keyword[keyword.find(','):]                    
                    if(len(keyword) > 1):                         #if exist with some words. 
                        keyword = keyword[1:]
                        #print "dateTime: " + keyword
                        dateTime = keyword
                else:
                    #print "teamB: " + keyword
                    teamB = findTeam(keyword)
                
        else:
            #print keyword
            teamA = findTeam(keyword)   

        return_keyword = []
        #print "@teamA: " + teamA
        #print "@teamB: " + teamB
        #print "@dateTime: " + dateTime

        if(teamA==' '):
            teamA=''
        if(teamB==' '):
            teamB=''
        if(dateTime==' '):
            dateTime=''
        else:
            dateTime = dateTime.replace(" ","")
        
        c = conn.cursor()

        if(found_teamB):
            sql_str = ""
            if(int(teamA) <= int(teamB)):                
                sql_str = "select  b.name, c.name, a.url, a.date, a.good_cnt, a.bad_cnt, a.uuid from game a, name_collection_eng b, name_collection_eng c where 1=1"
                sql_str += " and a.team_a = '"+ teamA + "' and a.team_b = '"+ teamB +"'"
            else:
                sql_str = "select  c.name, b.name, a.url, a.date, a.good_cnt, a.bad_cnt, a.uuid from game a, name_collection_eng b, name_collection_eng c where 1=1"
                sql_str += " and a.team_a = '"+ teamB + "' and a.team_b = '"+ teamA +"'"
            sql_str += " and a.date like '%"+ dateTime +"%'"
            sql_str += " and a.team_a = b.code and a.team_b = c.code order by a.date desc"
        else:
            sql_str = "select * from "
            sql_str += " ("
            sql_str += " select  b.name, c.name, a.url, a.date, a.good_cnt, a.bad_cnt, a.uuid from game a, name_collection_eng b, name_collection_eng c where 1=1"
            sql_str += " and a.team_a = '"+ teamA + "'"
            sql_str += " and a.team_a = b.code and a.team_b = c.code "
            sql_str += " and a.date like '%"+ dateTime +"%'"
            sql_str += " union all"
            sql_str += " select  c.name, b.name, a.url, a.date, a.good_cnt, a.bad_cnt, a.uuid from game a, name_collection_eng b, name_collection_eng c where 1=1"
            sql_str += " and a.team_b = '"+ teamA + "'"
            sql_str += " and a.team_a = b.code and a.team_b = c.code "
            sql_str += " and a.date like '%"+ dateTime +"%'"
            sql_str += " )"
            sql_str += " order by date desc"
        
        #print(sql_str)

        c.execute(sql_str);
        results = c.fetchall()

        if(len(results) < 1):
            flg = True
            url_s = "NEW"
            sql_str = "select name from name_collection_eng where code = '"+ teamA +"'"
            c.execute(sql_str)
            result = c.fetchone()
            if(result==None):
                teamA_s = ''
                flg = False
            else:
                teamA_s = result[0]

            sql_str = "select name from name_collection_eng where code = '"+ teamB +"'"
            c.execute(sql_str)
            result = c.fetchone()
            if(result==None):
                teamB_s = ''
                flg = False
            else:
                teamB_s = result[0]

            if(validateDateFormat(dateTime)==False):
                flg = False

            if(flg==False):
                url_s = "DNA" #Do Not Anything

            r = {"teamA":teamA_s, "teamB":teamB_s, "url" : url_s, "dateTime" : dateTime }
            return_keyword.append(r)
        else:
            for result in results:            
                r = {"teamA":result[0], "teamB":result[1], "url" : "http://"+result[2], "dateTime" : result[3], "goodCnt" : result[4], "badCnt" : result[5], "uuid" : result[6] }
                return_keyword.append(r)
   
        #return callback+"("+json.dumps(return_keyword)+")"
        return json.dumps(return_keyword)

class regNew(resource.Resource):
    isLeaf = True

    def render_POST(self, request):
        request.setHeader("content-type", "application/json")
        #print request.args
        teamA_s = (request.args['teamA'][0]).decode('utf-8')
        teamB_s = (request.args['teamB'][0]).decode('utf-8')
        date = (request.args['date'][0]).decode('utf-8')
        url = (request.args['url'][0]).decode('utf-8')
        url = url.lower()
        url = url.replace('http://','')
        uuidNew = uuid.uuid1()

        teamA = ''
        teamB = ''

        c = conn.cursor()

        sql_str = "select code from name_collection_eng where name = '"+ teamA_s +"'"
        c.execute(sql_str)
        result = c.fetchone()
        if(result==None):
            return 'False'
        else:
            teamA = result[0]

        sql_str = "select code from name_collection_eng where name = '"+ teamB_s +"'"
        c.execute(sql_str)
        result = c.fetchone()
        if(result==None):
            return 'False'
        else:
            teamB = result[0]

        try:
            if(int(teamA) <= int(teamB)):
                sql_str = "insert into game('team_a','team_b','date','url','uuid') values (%d,%d,'%s','%s','%s')" %(int(teamA), int(teamB), date, url, str(uuidNew) )
            else:
                sql_str = "insert into game('team_a','team_b','date','url','uuid') values (%d,%d,'%s','%s','%s')" %(int(teamB), int(teamA), date, url, str(uuidNew) )
            c.execute(sql_str)
            conn.commit()
        except sqlite3.Error, e:
            print "DB Error %s:" % e.args[0]
            return 'False'

        return json.dumps({"teamA":teamA_s, "teamB":teamB_s, "url" : url, "dateTime" : date, "uuid": str(uuidNew)})

class updateGoodBadCnt(resource.Resource):
    isLeaf = True

    def render_POST(self, request):
        request.setHeader("content-type", "application/json")
        uuidGet = (request.args['uuid'][0]).decode('utf-8')
        goodBadFlg = (request.args['goodBadFlg'][0]).decode('utf-8')        

        c = conn.cursor()

        try:
            if(goodBadFlg == 'good'):
                sql_str = "update game set good_cnt = ( ( select good_cnt from game where uuid = '%s' ) + 1 )where uuid = '%s'" %(uuidGet, uuidGet)
            else:
                sql_str = "update game set bad_cnt = ( ( select bad_cnt from game where uuid = '%s' ) + 1 )where uuid = '%s'" %(uuidGet, uuidGet)
            c.execute(sql_str)
            conn.commit()
        except sqlite3.Error, e:
            print "DB Error %s:" % e.args[0]
            return 'False'

        return "{}"

class updateGameUrl(resource.Resource):
    isLeaf = True

    def render_POST(self, request):
        request.setHeader("content-type", "application/json")
        uuidGet = (request.args['uuid'][0]).decode('utf-8')
        url = (request.args['url'][0]).decode('utf-8')
        url = url.lower()
        url = url.replace('http://','')

        c = conn.cursor()

        try:
            sql_str = "update game set url = '%s' where uuid = '%s'" %(url, uuidGet)           
            c.execute(sql_str)
            conn.commit()
        except sqlite3.Error, e:
            print "DB Error %s:" % e.args[0]
            return 'False'

        return "{}"        

#root = resource.Resource()
root = static.File("./html")
root.putChild("searchEngine", searchEngine())
root.putChild("regNew", regNew())
root.putChild("updateGoodBadCnt", updateGoodBadCnt())
root.putChild("updateGameUrl", updateGameUrl())
root.putChild("test", test())
#root.default = static.File("./var")


factory = server.Site(root) 
reactor.listenTCP(80, factory)

reactor.run()