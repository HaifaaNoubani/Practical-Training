import glob
import psycopg2 
try:
    connection = psycopg2.connect(host="localhost", port = 5432, database="postgres", user="postgres", password="Buthaina204")

    cursor = connection.cursor()

    filePath = r'C:\Users\user\Desktop\Practical-Training\code-parser\parse'
    all_files = glob.glob(filePath + "\*.csv")
    for path in all_files:
        with open(path,'r',encoding = "UTF-8") as file:
            postgres_insert_student = "INSERT INTO public.student(name, avg, branch_id, school_id, directorate_id, year) VALUES (%s, %s, %s, %s, %s, %s);"
            postgres_insert_branch = "INSERT INTO public.branch(branch) VALUES (%s);"
            postgres_insert_directorate = "INSERT INTO public.directorate(directorate) VALUES (%s);"
            postgres_insert_school = "INSERT INTO public.school(school_name) VALUES (%s);"
            #file.readline()

            for line in file:
                 row=line.split(',')
                 try:
                    cursor.execute("SELECT branch_id FROM public.branch where branch = %s;",(row[2],))
                    if bool(cursor.rowcount)== False:
                          record_to_insert = (row[2],)
                          cursor.execute(postgres_insert_branch,record_to_insert)
                          connection.commit()
                    thebranch=cursor.fetchone()
                    cursor.execute("SELECT school_id From public.school WHERE school_name = %s;",(row[3],))
                    if  bool(cursor.rowcount)== False:
                          record_to_insert = (row[3],)
                          cursor.execute(postgres_insert_school,record_to_insert)
                          connection.commit()
                    school_name=cursor.fetchone()
                    cursor.execute("SELECT directorate_id From public.directorate WHERE directorate = %s;",(row[4],))
                    if  bool(cursor.rowcount)== False:
                          record_to_insert = (row[4],)
                          cursor.execute(postgres_insert_directorate,record_to_insert)
                          connection.commit()
                    thedirectorate=cursor.fetchone()
                    record_to_insert = (row[0],row[1],thebranch[0],school_name[0],thedirectorate[0],row[5])
                    cursor.execute(postgres_insert_student,record_to_insert)
                 except Exception as error:
                     print("Failed to insert record into tawjihi table", error)
                     continue
                 
        file.close()


except (Exception, psycopg2.Error) as error :
    if(connection):
        print("Failed to insert record into tawjihi table", error)

finally:
    #closing database connection.
    if(connection):
        connection.commit()
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
