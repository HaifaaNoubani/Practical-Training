
from flask import Flask
import json
import psycopg2 


app = Flask(__name__)
@app.route('/<name>')
def my_search(name):
    try:
        connection = psycopg2.connect(host="localhost", port = 5432, database="postgres", user="postgres", password="Buthaina204")
        cursor = connection.cursor()
        row=name.split(' ')
        search ="select name,avg,public.branch.branch_name,public.school.school_name,public.directorate.directorate_name,year from public.student "
        search += "INNER JOIN public.directorate ON student.directorate_id = public.directorate.directorate_id "
        search += "INNER JOIN public.school ON student.school_id = public.school.school_id "
        search += "INNER JOIN public.branch ON student.branch_id = public.branch.branch_id "
        search +="where name like '%"+row[0]+"%'"
        row.pop(0)
        for my in row:
           if len(row)>=1:
                search += "and name like '%"+my+"%' "
        search +=";"
        cursor.execute(search)
        records=cursor.fetchall()
        myList=[]
        for myRow in records:
            myList.append({'name':myRow[0],'avg':str(myRow[1]),'branch':myRow[2],'school':myRow[3],'directorate':myRow[4],'year':myRow[5]})
       # y = json.dumps(myList,sort_keys=True)
       # print(y)

    except (Exception, psycopg2.Error) as error :
        if(connection):
           print("Failed to insert record into tawjihi table", error)

    finally:
    #closing database connection.
      if(connection):
         cursor.close()
         connection.close()
         print("PostgreSQL connection is closed")

    return json.dumps(myList, ensure_ascii=False)
   
@app.route('/search/<id>')
def my_searchId(id):
    try:
        connection = psycopg2.connect(host="localhost", port = 5432, database="postgres", user="postgres", password="Buthaina204")
        cursor = connection.cursor()
        search="select "
        search +="(select count(*) from public.student where avg>my.avg and year=my.year)yearRank,"
        search +="(select count(*) from public.student where avg>my.avg and year=my.year and school_id=my.school_id)schoolRank,"
        search +="(select count(*) from public.student where avg>my.avg and year=my.year and directorate_id=my.directorate_id)directorateRank,"
        search +="(select count(*) from public.student where avg>my.avg and year=my.year and branch_id=my.branch_id)branchRank "
        search +="from public.student my where id="+id+";" ;
        cursor.execute(search)
        record=cursor.fetchone()
        myList={'yearRank':record[0],'schoolRank':record[1],'directorateRank':record[2],'branchRank':record[3]}
    except (Exception, psycopg2.Error) as error :
        if(connection):
           print("Failed to insert record into tawjihi table", error)

    finally:
    #closing database connection.
      if(connection):
         cursor.close()
         connection.close()
         print("PostgreSQL connection is closed")
    return json.dumps(myList, ensure_ascii=False)