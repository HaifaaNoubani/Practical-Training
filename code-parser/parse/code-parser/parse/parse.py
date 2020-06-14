import glob
import psycopg2 
class HashTable():

    def __init__(self):
        # Initiate our array with empty values.
        self.dects = {}
    def myhash(self):
        """Get the index of our array for a specific string key"""
        lengths=len(self.dects)+1
        return lengths
    def add(self, key, value):
        """Add a value to our array by its key"""
        if self.dects.get(key) is None:
            self.dects[key]=value
    def gets(self, key):
        """Get a value by key"""
        if self.dects[key] is None:
            return False
        else:
            return self.dects.get(key)

branchHash=HashTable()
schoolHash=HashTable()
directorateHash=HashTable()
try:
    connection = psycopg2.connect(host="localhost", port = 5432, database="postgres", user="postgres", password="Buthaina204")

    cursor = connection.cursor()
    try:
        cursor.execute("select * from public.branch")
        branch_records = cursor.fetchall() 
        for row in branch_records:
            branchHash.add(row[1],row[0])

        cursor.execute("select * from public.school")
        school_records = cursor.fetchall() 
        for row in school_records:
            schoolHash.add(row[1],row[0])

        cursor.execute("select * from public.directorate")
        directorate_records = cursor.fetchall() 
        for row in directorate_records:
            directorateHash.add(row[1],row[0])
    except (Exception, psycopg2.Error) as error :
           print ("Error while fetching data from PostgreSQL", error)
    filePath = r'C:\Users\user\Desktop\Practical-Training\code-parser\parse'
    all_files = glob.glob(filePath + "\*.csv")
    for path in all_files:
        with open(path,'r',encoding = "UTF-8") as file:
            postgres_insert_student = "INSERT INTO public.student(name, avg, branch_id, school_id, directorate_id, year) VALUES (%s, %s, %s, %s, %s, %s);"
            postgres_insert_branch = "INSERT INTO public.branch(branch_id,branch_name) VALUES (%s,%s);"
            postgres_insert_directorate = "INSERT INTO public.directorate(directorate_id,directorate_name) VALUES (%s,%s);"
            postgres_insert_school = "INSERT INTO public.school(school_id,school_name) VALUES (%s,%s);"
            #file.readline()

            for line in file:
                 row=line.split(',')
                 try:
                   
                    if branchHash.gets(row[2])== False:
                          branchHash.add(row[2],branchHash.myhash())
                          record_to_insert=(branchHash.gets(row[2]),row[2])
                          cursor.execute(postgres_insert_branch,record_to_insert)
                          
                    if  schoolHash.gets(row[3])== False:
                        schoolHash.add(row[3],schoolHash.myhash())
                        record_to_insert=(schoolHash.gets(row[3]),row[3])
                        cursor.execute(postgres_insert_school,record_to_insert)

                    if  directorateHash.gets(row[4])== False:
                        directorateHash.add(row[4],directorateHash.myhash())
                        record_to_insert=(directorateHash.gets(row[4]),row[4])
                        cursor.execute(postgres_insert_directorate,record_to_insert)

                    record_to_insert = (row[0],row[1],branchHash.gets(row[2]),schoolHash.gets(row[3]),directorateHash.gets(row[4]),row[5])
                    cursor.execute(postgres_insert_student,record_to_insert)
                    print("yes to insert record into tawjihi table", error)
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
